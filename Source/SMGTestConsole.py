from kafka import KafkaProducer
from kafka import KafkaConsumer
from Source.SMGConfigMgr import SMGConfigMgr
from Source.StockMarketDB import StockMarketDB
from Source.SMGOrderManager import SMGOrderManager
from Source.SMGLogger import SMGLogger
from Source.UserManager import UserManager
from Source.SMGOrderTypes import SMOrderTypes

import threading
import sys
import os
import time

def clearScreen():
    os.system('cls')

def mainMenu():
    clearScreen()
    print("1. Connect")
    print("2. Add User")
    print("3. Add Order")
    print("4. View All Orders")
    print("5. View All Fills")
    print("6. Exit")


def getUser(userMgr):

    clearScreen()
    username = input("Enter UserName: ")
    password = input("Enter Password: ")
    user = userMgr.getUser(username)
    if user is None:
        print("can't fine user " + username)
        return None
    if password != user.Password:
        print("Passowrd is incorrect")
        return None

    return user.UserId


def createOrder(orderMgr, userId):

    clearScreen()
    symbol = input("Enter Symbol: ")
    side = input("Enter Side(Buy or Sell): ")
    qty = float(input("Enter Qty: "))

    order = orderMgr.createOrder("", "", symbol, side, qty, SMOrderTypes.Market.value, 0,
                                 "Day", "", "", userId, "CRYPTO")

    return order


def run(logger, dbMgr, orderMgr, userMgr):
    logger.info("SMGTestConsole.run method.  Getting ready for user to enter commands!!!!!")
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    userId = None
    while (1):
        mainMenu()
        response = input("Enter Selection: ")
        val = int(response)
        if val == 1:
            userId = getUser(userMgr)
            if userId is None:
                print("Could not validate user.  Existing")
                break
        elif val == 2:
            print("Not Implimented yet")
        elif val == 3:
            if userId is None:
                print("Can't enter order until you login")
            else:
                order = createOrder(orderMgr, userId)
                if order is None:
                    print("Not able to create the Order")
                print("Was able to create Order.  OrdeId is " + order.OrderId)
                #producer.send("NewOrder",str(order).encode('utf-8'))
                logger.info("Send out order " + str(order))
        elif val == 4:
            print("Not Implimented yet")
        elif val == 5:
            print("Not Implimented yet")
        elif val == 6:
            print("Thanks for using it")
            break
        time.sleep(2)


def main():
    if len(sys.argv) != 2:
        print("usage: SMGTestConsole.py <configfile>")
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

    dbMgr = StockMarketDB(user, password, host)
    dbMgr.connect()
    dbMgr.changeDb(database)

    orderMgr = SMGOrderManager(suffix, orderSeq, fillSeq, systemName)
    logger = SMGLogger(logFile, logLevel)

    logger.info("Started up SMGTestConsole")
    userMgr = UserManager(host, user, password, logger)
    userMgr.connect(database)

    run(logger, dbMgr, orderMgr, userMgr)


if __name__ == '__main__':

    main()
