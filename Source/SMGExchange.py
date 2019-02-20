from kafka import KafkaProducer
from kafka import KafkaConsumer
from Source.SMGOrderManager import SMGOrderManager
from Source.DBOrderManagerWriter import DBOrderManagerWriter
from Source.SMGConfigMgr import SMGConfigMgr
import sys
import datetime
from Source.SMGLogger import SMGLogger


class SMGExchange(object):

    def __init__(self, hostName, user, password, dbName, omSuffix, orderSeq, fillSeq, systemName, logName, logLevel):

        self.Orders = {}
        self.Fills = {}
        self.Bids = {}
        self.Offers = {}
        self.OM = SMGOrderManager(omSuffix, orderSeq, fillSeq, systemName)
        self.Producer = KafkaProducer(bootstrap_servers='localhost:9092')
        self.Consumer = KafkaConsumer(bootstrap_servers='localhost:9092', auto_offset_reset='earliest', consumer_timeout_ms=1000)
        self.DB = DBOrderManagerWriter(hostName, user, password, dbName)
        self.Logger = SMGLogger(logName, logLevel)

    def setFillSeq(self):

        sqlString = "select max(created) from smgfill where ordersystem = 'SMGExchange'"
        results =self.DB.Db.select(sqlString)
        if len(results) == 0:
            return
        created = ""
        for result in results:
            created = result[0]
        sqlString = "select fillId from smgfill where ordersystem = 'SMGExchange' and created ='%s'" % (created)
        results = self.DB.Db.select(sqlString)

        if len(results) == 0:
            self.Logger.info("Did not get back a fillId for SMGExchange.  Strange!!!")
            return
        fillId = ""
        for result in results:
            fillId = result[0]

        temp = fillId.split('-')
        if len(temp) != 2:
            self.Logger.info("Error trying to split fillId.  FillId is " + fillId)
            return

        self.OM.setFillSeq(int(temp[1]))

    def setOrderSeq(self):

        sqlString = "select max(lastupdate) from smgorder where ordersystem = 'SMGExchange'"
        results = self.DB.Db.select(sqlString)
        if len(results) == 0:
            return
        lastupdate = ""
        for result in results:
            lastupdate = result[0]
        sqlString = "select orderId from smgorder where ordersystem = 'SMGExchange' and lastupdate = '%s'" % (lastupdate)

        results = self.DB.Db.select(sqlString)
        if len(results) == 0:
            self.Logger.info("Did not get back a orderId for SMGExchange.  Strange!!!")
            return
        orderId = ""
        for result in results:
            orderId = result[0]

        temp = orderId.split('-')
        if len(temp) != 2:
            self.Logger.info("Error trying to split OrderId.  orderId is " + orderId)
            return

        self.OM.setOrderSeq(int(temp[1]))

    def processBidOffer(self,message):

        temp = message.split(',')
        if len(temp) == 1:
            return

        symbol = temp[1]
        bid = float(temp[2])
        offer = float(temp[3])
        self.Bids[symbol] = bid
        self.Offers[symbol] = offer
        self.Logger.info("Update Bid/Offer for " + symbol + " " + str(bid) + " X " + str(offer))

    def getPrice(self, symbol, side):

        if side == "Buy":
            if symbol in self.Offers.keys():
                return self.Offers[symbol]
        else:
            if symbol in self.Bids.keys():
                return self.Bids[symbol]

        return 1

    def processOrder(self,message):

        order = self.OM.createOrderFromMsg(message)
        self.DB.saveNewOrder(order)
        price = self.getPrice(order.Symbol, order.Side)
        fill = self.OM.createFill("", order.OrderId,order.Qty, price, order.ExtOrderId, datetime.datetime.now())
        self.DB.saveNewFill(fill)
        self.DB.updateOrder(order)

        topic = order.ExtSystem + "Fill"
        self.Logger.info("Sending fill - Topic " + topic + " - " + str(fill))
        self.Producer.send(topic, str(fill).encode('utf-8'))

    def run(self):

        self.setFillSeq()
        self.setOrderSeq()
        self.Logger.info("Subscribing to GDAXFeed and SMGExchangeOrder")
        self.Consumer.subscribe(['GDAXFeed', 'SMGExchangeOrder'])

        while 1:
            for message in self.Consumer:
                msg = message[6].decode("utf-8")
                if message[0] == "GDAXFeed":
                    self.processBidOffer(msg)
                elif message[0] == "SMGExchangeOrder":
                    self.Logger.info("Got an order - " + msg)
                    self.processOrder(msg)


def main():

    if len(sys.argv) != 2:
        print("usage: SMGExchange.py <configfile>")
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
    logFile = config.getConfigItem("Logging", "filename")
    logLevel = config.getConfigItem("Logging", "loglevel")

    if host is None or user is None or password is None or database is None or suffix is None \
        or orderSeq is None or fillSeq is None or systemName is None or logFile is None or logLevel is None:
        print("Invalid configuration data.  Please check your configuration")
        exit(1)

    client = SMGExchange(host, user, password, database, suffix, orderSeq, fillSeq, systemName, logFile, logLevel)
    client.run()


if __name__ == '__main__':

    main()

