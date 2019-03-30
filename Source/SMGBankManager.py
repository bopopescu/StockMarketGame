from Source.BankManager import BankManager
from Source.SMGLogger import SMGLogger
from kafka import KafkaConsumer
from kafka import KafkaProducer
from Source.SMGOrderManager import SMGOrderManager
import sys
from Source.SMGConfigMgr import SMGConfigMgr
from Source.DBOrderManagerWriter import DBOrderManagerWriter
from Source.OrderSequenceQuery import OrderSequenceQuery
from Source.PricingManager import PricingManager


class SMGBankManager(object):

    def __init__(self, host, user, password, database, logFile, logLevel, omSuffix, systemName):

        self.Logger = SMGLogger(logFile, logLevel)
        self.BankManager = BankManager(host, user, password, self.Logger)
        self.OrderMgr = SMGOrderManager(omSuffix, 0, 0, systemName)
        self.DbOmWriter = DBOrderManagerWriter(host, user, password, database)
        self.Database = database
        self.PricingMgr = PricingManager()
        self.ExtOrderId = 0

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("Going to exit")

    def __enter__(self):

        self.BankManager.connect(self.Database)
        self.Producer = KafkaProducer(bootstrap_servers='localhost:9092')
        self.Consumer = KafkaConsumer(bootstrap_servers='localhost:9092', auto_offset_reset='earliest',
                                      consumer_timeout_ms=1000)
        self.BankManager.UserMgr.loadInitialData()
        self.Consumer.subscribe(['GDAXFeed', 'SMGNewOrder', 'SMGBankManagerFill'])
        return self

    def processFill(self, message):

        try:
            if not self.OrderMgr.isValidFill(message):
                return

            fill = self.OrderMgr.createFillFromMsg(message, self.UserId)
            if fill is None:
                return

            self.Logger.info("Got Execution -" + str(fill))
            self.DbOmWriter.saveNewFill(fill)

            order = self.OrderMgr.getOrder(fill.OrderId)
            if order is None:
                return

            self.DbOmWriter.updateOrder(order)
            self.Producer.send("SMGNewFill", str(fill).encode('utf-8'))

            self.updatePositions(order, fill)
        except Exception:
            self.Logger.error("Error processing Fill")

    def updatePositions(self, order, fill):

        buyPosition, sellPosition = self.BankManager.updatePosition(order.UserId, order.Symbol, fill.Qty,
                                                                    fill.Price, order.Side, order.SecType)
        if buyPosition is not None:
            self.Producer.send("SMGPosition", str(buyPosition).encode('utf-8'))
        if sellPosition is not None:
            self.Producer.send("SMGPosition", str(sellPosition).encode('utf-8'))

    def processOrder(self, message):

        try:
            temp = message.split(',')
            if len(temp) != 18:
                self.Logger.error("Invalid Order Message")
                return "0,ERROR,Invalid Order Message"

            extOrderId = temp[0]
            etemp = extOrderId.split('-')
            if len(etemp) != 2:
                self.Logger.info("Error processing order parsing extOrderId - " + extOrderId)
                return extOrderId + ",ERROR,Error processing order parsing"

            if int(etemp[1]) <= self.ExtOrderId:
                return "IGNORE"

            userId = int(temp[16])
            order = self.OrderMgr.createOrderFromMsg(message, userId)
            price = self.PricingMgr.getPrice(order.Symbol, order.Side)

            if price == 0:
                return order.ExtOrderId + ",ERROR,Can't get valid price to evaluate"

            if self.BankManager.canTradeCrypto(order.UserId, order.Symbol, order.Side, order.Qty, price) is False:
                return order.ExtOrderId + ",ERROR,Not enough funds to trade"

            self.DbOmWriter.saveNewOrder(order)
            self.Logger.info("Sending Order - " + str(order))
            self.Producer.send('SMGExchangeOrder', str(order).encode('utf-8'))
            return order.ExtOrderId + ",SUCCESS,Sent Order"
        except Exception as e:
            self.Logger.error("Error processing Order message " + message + " Error:" + str(e))
            return "0,ERROR,Error processing Order message "

    def run(self):

        while 1:
            for message in self.Consumer:
                msg = message[6].decode("utf-8")
                if message[0] == "GDAXFeed":
                    self.PricingMgr.processPriceMsg(msg)
                elif message[0] == "SMGNewOrder":
                    self.Logger.info("Got an order - " + msg)
                    text = self.processOrder(msg)
                    if text != "IGNORE":
                        self.Producer.send('SMGNewOrderResponse', text.encode('utf-8'))
                elif message[0] == "SMGBankManagerFill":
                    self.processFill(message)


def main():
    if len(sys.argv) != 2:
        print("usage: SMGBankManager.py <configfile>")
        exit(1)

    config = SMGConfigMgr()
    config.load(sys.argv[1])

    host = config.getConfigItem("DatabaseInfo", "host")
    user = config.getConfigItem("DatabaseInfo", "user")
    password = config.getConfigItem("DatabaseInfo", "passwd")
    database = config.getConfigItem("DatabaseInfo", "db")
    suffix = config.getConfigItem("OrderManager", "omsuffix")
    systemName = config.getConfigItem("OrderManager", "systemname")
    logFile = config.getConfigItem("Logging", "filename")
    logLevel = config.getConfigItem("Logging", "loglevel")

    if host is None or user is None or password is None or database is None or suffix is None \
        or systemName is None or logFile is None or logLevel is None:
        print("Invalid configuration data.  Please check your configuration")
        exit(1)

    with SMGBankManager(host, user, password, database, logFile, logLevel, suffix, systemName) as bankMgr:
        with OrderSequenceQuery(host, user, password, database) as seq:
            bankMgr.OrderMgr.setOrderSeq(seq.getOrderSeq(systemName))
            bankMgr.OrderMgr.setFillSeq(seq.getFillSeq(systemName))
            bankMgr.ExtOrderId = seq.getExtOrderSeq(systemName)
        bankMgr.run()


if __name__ == '__main__':

    main()
