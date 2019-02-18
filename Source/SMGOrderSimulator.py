from kafka import KafkaProducer
from kafka import KafkaConsumer
from Source.SMGOrderManager import SMGOrderManager
from Source.SMGOrderTypes import SMOrderTypes
import threading
from Source.DBOrderManagerWriter import DBOrderManagerWriter
import sys
from Source.SMGConfigMgr import SMGConfigMgr


class SMGOrderSimulator(object):

    def __init__(self, hostName, user, password, dbName, omSuffix, orderSeq, fillSeq, systemName, defaultSide):

        self.Producer = KafkaProducer(bootstrap_servers='localhost:9092')
        self.Consumer = KafkaConsumer(bootstrap_servers='localhost:9092', auto_offset_reset='earliest', consumer_timeout_ms=1000)
        self.Timer = threading.Timer(10, self.sendOrder)
        self.OM = SMGOrderManager(omSuffix, orderSeq, fillSeq, systemName)
        self.Side = defaultSide
        self.DB = DBOrderManagerWriter(hostName, user, password, dbName)

    def setSide(self):

        if self.Side == "Buy":
            self.Side = "Sell"
        else:
            self.Side = "Buy"

    def sendOrder(self):

        self.setSide()
        order = self.OM.createOrder("","","BTC-USD",self.Side,100,SMOrderTypes.Market.value, 0, "Day","","")
        self.DB.saveNewOrder(order)
        print("Sending Order - " + str(order))
        self.Producer.send('SMGExchangeOrder', str(order).encode('utf-8'))
        self.Timer = threading.Timer(10, self.sendOrder)
        self.Timer.start()

    def processFill(self, message):

        fill = self.OM.createFillFromMsg(message)
        if fill is None:
            return

        print("Got Execution -" + str(fill))
        self.DB.saveNewFill(fill)

        order = self.OM.getOrder(fill.OrderId)
        if order is None:
            return
        self.DB.updateOrder(order)

    def run(self):

        self.Consumer.subscribe(['SimulatorFill'])
        self.Timer.start()

        while 1:
            for message in self.Consumer:
                msg = message[6].decode("utf-8")
                self.processFill(msg)

def main():

    if len(sys.argv) != 2:
        print("usage: SMGOrderSimulator.py <configfile>")
        exit(1)

    config = SMGConfigMgr()
    config.load(sys.argv[1])

    host = config.getConfigItem("DatabaseInfo", "host")
    user = config.getConfigItem("DatabaseInfo", "user")
    password = config.getConfigItem("DatabaseInfo", "passwd")
    database = config.getConfigItem("DatabaseInfo", "db")
    suffix = config.getConfigItem("OrderManager", "omsuffix")
    orderSeq = int(config.getConfigItem("OrderManager", "orderseq"))
    fillSeq = int(config.getConfigItem("OrderManager", "fillseq"))
    systemName = config.getConfigItem("OrderManager", "systemname")
    defaultSide = config.getConfigItem("OrderManager", "defaultside")

    if host is None or user is None or password is None or database is None or suffix is None \
        or orderSeq is None or fillSeq is None or systemName is None or defaultSide is None:
        print("Invalid configuration data.  Please check your configuration")
        exit(1)

    simulator = SMGOrderSimulator(host, user, password, database, suffix, orderSeq, fillSeq, systemName, defaultSide)
    simulator.run()


if __name__ == '__main__':

    main()
