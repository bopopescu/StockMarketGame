from kafka import KafkaProducer
from kafka import KafkaConsumer
from Source.SMGOrderManager import SMGOrderManager
from Source.SMGOrderTypes import SMOrderTypes
import threading
from Source.DBOrderManagerWriter import DBOrderManagerWriter
import sys
from Source.SMGConfigMgr import SMGConfigMgr
from Source.SMGLogger import SMGLogger
from Source.OrderSequenceQuery import OrderSequenceQuery
import random


class SMGOrderSimulator(object):

    def __init__(self, hostName, user, password, dbName, omSuffix, orderSeq, fillSeq, systemName, defaultSide, logName, logLevel):

        self.Producer = KafkaProducer(bootstrap_servers='localhost:9092')
        self.Consumer = KafkaConsumer(bootstrap_servers='localhost:9092', auto_offset_reset='earliest', consumer_timeout_ms=1000)
        self.Timer = threading.Timer(10, self.sendOrder)
        self.OM = SMGOrderManager(omSuffix, orderSeq, fillSeq, systemName)
        self.Side = defaultSide
        self.DbOmWriter = DBOrderManagerWriter(hostName, user, password, dbName)
        self.Logger = SMGLogger(logName, logLevel)
        self.SimTickers = ['BTC-USD', 'ETH-USD', 'LTC-USD', 'BCH-USD', 'ZRX-USD']
        self.SimTickerCount = 0
        self.UserId = -1

    def setUserId(self):

        sqlText = "select userid from smguser where username ='SMGOrderSimulator'"

        results = self.DbOmWriter.Db.select(sqlText)
        for result in results:
            self.UserId = result[0]
            return True

        return False

    def setSide(self):

        if self.Side == "Buy":
            self.Side = "Sell"
        else:
            self.Side = "Buy"

    def getSymbol(self):

        symbol = self.SimTickers[self.SimTickerCount]
        self.SimTickerCount += 1

        if self.SimTickerCount == len(self.SimTickers):
            self.SimTickerCount = 0

        return symbol

    def getQty(self):

        qty = random.randrange(100,1000,10)

        return qty

    def sendOrder(self):

        try:
            self.setSide()
            symbol = self.getSymbol()
            qty = self.getQty()
            order = self.OM.createOrder("","",symbol,self.Side,qty,SMOrderTypes.Market.value, 0, "Day","","",self.UserId,"CRYPTO")
            self.DbOmWriter.saveNewOrder(order)
            self.Logger.info("Sending Order - " + str(order))
            self.Producer.send('SMGExchangeOrder', str(order).encode('utf-8'))
            self.Timer = threading.Timer(1, self.sendOrder)
            self.Timer.start()
        except Exception:
            self.Logger.error("Error sending Order")

    def isValidFill(self, message):

        temp = message.split(',')
        if len(temp) != 9:
            return False

        temp2 = temp[6].split('-')
        if len(temp2) != 2:
            return False

        if int(temp2[1]) <= self.OM.FillCounter:
            return False

        return True

    def processFill(self, message):

        try:
            if not self.isValidFill(message):
                return
            fill = self.OM.createFillFromMsg(message, self.UserId)
            if fill is None:
                return

            self.Logger.info("Got Execution -" + str(fill))
            self.DbOmWriter.saveNewFill(fill)

            order = self.OM.getOrder(fill.OrderId)
            if order is None:
                return
            self.DbOmWriter.updateOrder(order)
        except Exception:
            self.Logger.error("Error processing Fill")

    def run(self):

        if self.setUserId() is False:
            self.Logger.error("Not able to find userId for SMGOrderSimulator")
            return

        self.Logger.info("Subscribe to SimulatorFill")
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
    logFile = config.getConfigItem("Logging", "filename")
    logLevel = config.getConfigItem("Logging", "loglevel")

    if host is None or user is None or password is None or database is None or suffix is None \
        or orderSeq is None or fillSeq is None or systemName is None or defaultSide is None \
        or logFile is None or logLevel is None:
        print("Invalid configuration data.  Please check your configuration")
        exit(1)

    simulator = SMGOrderSimulator(host, user, password, database, suffix, orderSeq, fillSeq, systemName, defaultSide, logFile, logLevel)
    with OrderSequenceQuery(host, user, password, database) as seq:
        simulator.OM.setOrderSeq(seq.getOrderSeq(systemName))
        simulator.OM.setFillSeq(seq.getFillSeq(systemName))

    simulator.run()


if __name__ == '__main__':

    main()
