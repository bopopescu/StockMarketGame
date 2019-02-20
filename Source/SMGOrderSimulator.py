from kafka import KafkaProducer
from kafka import KafkaConsumer
from Source.SMGOrderManager import SMGOrderManager
from Source.SMGOrderTypes import SMOrderTypes
import threading
from Source.DBOrderManagerWriter import DBOrderManagerWriter
import sys
from Source.SMGConfigMgr import SMGConfigMgr
from Source.SMGLogger import SMGLogger
from Source.StockMarketDB import StockMarketDB


class SMGOrderSimulator(object):

    def __init__(self, hostName, user, password, dbName, omSuffix, orderSeq, fillSeq, systemName, defaultSide, logName, logLevel):

        self.Producer = KafkaProducer(bootstrap_servers='localhost:9092')
        self.Consumer = KafkaConsumer(bootstrap_servers='localhost:9092', auto_offset_reset='earliest', consumer_timeout_ms=1000)
        self.Timer = threading.Timer(10, self.sendOrder)
        self.OM = SMGOrderManager(omSuffix, orderSeq, fillSeq, systemName)
        self.Side = defaultSide
        self.DbOmWriter = DBOrderManagerWriter(hostName, user, password, dbName)
        self.Logger = SMGLogger(logName, logLevel)

    def setFillSeq(self):

        sqlString = "select max(created) from smgfill where refId like 'SIM%'"
        results =self.DbOmWriter.Db.select(sqlString)
        if len(results) == 0:
            return
        created = ""
        for result in results:
            created = result[0]
        sqlString = "select refId from smgFill where created='%s'" % (created)
        results = self.DbOmWriter.Db.select(sqlString)

        if len(results) == 0:
            self.Logger.info("Did not get back a refId for SIM.  Strange!!!")
            return
        fillId = ""
        for result in results:
            if "SIM" in result[0]:
                fillId = result[0]

        temp = fillId.split('-')
        if len(temp) != 2:
            self.Logger.info("Error trying to split fillId.  FillId is " + fillId)
            return

        self.OM.setFillSeq(int(temp[1]))

    def setOrderSeq(self):

        sqlString = "select max(lastupdate) from smgorder where ordersystem = 'Simulator'"
        results = self.DbOmWriter.Db.select(sqlString)
        if len(results) == 0:
            return
        lastupdate = ""
        for result in results:
            lastupdate = result[0]
        sqlString = "select orderId from smgorder where ordersystem = 'Simulator' and lastupdate = '%s'" % (lastupdate)

        results = self.DbOmWriter.Db.select(sqlString)
        if len(results) == 0:
            self.Logger.info("Did not get back a orderId for Simulator.  Strange!!!")
            return
        orderId = ""
        for result in results:
            orderId = result[0]

        temp = orderId.split('-')
        if len(temp) != 2:
            self.Logger.info("Error trying to split OrderId.  orderId is " + orderId)
            return

        self.OM.setOrderSeq(int(temp[1]))

    def setSide(self):

        if self.Side == "Buy":
            self.Side = "Sell"
        else:
            self.Side = "Buy"

    def sendOrder(self):

        self.setSide()
        order = self.OM.createOrder("","","BTC-USD",self.Side,100,SMOrderTypes.Market.value, 0, "Day","","")
        self.DbOmWriter.saveNewOrder(order)
        self.Logger.info("Sending Order - " + str(order))
        self.Producer.send('SMGExchangeOrder', str(order).encode('utf-8'))
        self.Timer = threading.Timer(10, self.sendOrder)
        self.Timer.start()

    def isValidFill(self, message):

        temp = message.split(',')
        if len(temp) != 8:
            return False

        temp2 = temp[6].split('-')
        if len(temp2) != 2:
            return False

        if int(temp2[1]) <= self.OM.FillCounter:
            return False

        return True

    def processFill(self, message):

        if not self.isValidFill(message):
            return
        fill = self.OM.createFillFromMsg(message)
        if fill is None:
            return

        self.Logger.info("Got Execution -" + str(fill))
        self.DbOmWriter.saveNewFill(fill)

        order = self.OM.getOrder(fill.OrderId)
        if order is None:
            return
        self.DbOmWriter.updateOrder(order)

    def run(self):

        self.setFillSeq()
        self.setOrderSeq()
        self.Logger.info("Subscribe to SimulatorFill")
        self.Consumer.subscribe(['SimulatorFill'])
        #self.Timer.start()

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
    simulator.run()


if __name__ == '__main__':

    main()
