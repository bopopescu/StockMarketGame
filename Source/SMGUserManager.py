from kafka import KafkaConsumer
from kafka import KafkaProducer
import sys
from Source.SMGConfigMgr import SMGConfigMgr
from Source.SMGLogger import SMGLogger
from Source.UserManager import UserManager


class SMGUserManager(object):

    def __init__(self, host, user, password, logFile, logLevel):

        self.Logger = SMGLogger(logFile, logLevel)
        self.UserManager = UserManager(host, user, password, self.Logger)
        self.Producer = None
        self.Consumer = None

    def connect(self):
        self.Producer = KafkaProducer(bootstrap_servers='localhost:9092')
        self.Consumer = KafkaConsumer(bootstrap_servers='localhost:9092', auto_offset_reset='earliest', consumer_timeout_ms=1000)

    def run(self, database):
        self.connect()
        self.UserManager.connect(database)
        self.Logger.info("Subscribe to SMGNewUser")
        self.Consumer.subscribe(['SMGNewUser'])
        self.UserManager.loadInitialData()

        recovering = True

        while 1:
            for message in self.Consumer:
                msg = message[6].decode("utf-8")
                user = self.UserManager.createUserObjectFromMessage(msg)
                if user is None:
                    continue
                if self.UserManager.doesUserExist(user.UserName):
                    if recovering is True:
                        continue
                    errorMsg = "User already exist.  Can't create user " + user.UserName
                    self.Logger.error(errorMsg)

                    self.Producer.send("UserAddFailed", errorMsg.encode('utf-8'))
                else:
                    self.UserManager.saveUser(user)
                    actuser = self.UserManager.getUser(user.UserName)
                    if actuser is None:
                        self.Logger.error("Error creating user")
                        retval = "User did not save " + user.UserName
                        self.Producer.send("UserAddFailed", retval.encode('utf-8'))
                    else:
                        self.UserManager.updateUserHistory(actuser, "NEW")
                        self.UserManager.createPortfolio(actuser, 15000000.00)
                        self.UserManager.createInitialPosition(actuser.UserId, "USD", 15000000.00)
                        self.Producer.send("NewUser", str(actuser).encode('utf-8'))
                        portfolio = self.UserManager.getPortfolio(actuser.UserId)
                        self.Producer.send("NewPortfolio", str(portfolio).encode('utf-8'))
                        position = self.UserManager.getPosition(actuser.UserId, "USD")
                        self.Producer.send("NewPosition", str(position).encode('utf-8'))

            recovering = False


def main():
    if len(sys.argv) != 2:
        print("usage: SMGUserManager.py <configfile>")
        exit(1)

    config = SMGConfigMgr()
    config.load(sys.argv[1])

    host = config.getConfigItem("DatabaseInfo", "host")
    user = config.getConfigItem("DatabaseInfo", "user")
    password = config.getConfigItem("DatabaseInfo", "passwd")
    database = config.getConfigItem("DatabaseInfo", "db")
    logFile = config.getConfigItem("Logging", "filename")
    logLevel = config.getConfigItem("Logging", "loglevel")

    if host is None or user is None or password is None or database is None or logFile is None or logLevel is None:
        print("Invalid configuration items.  Please check config file.")
        exit(1)

    userMgr = SMGUserManager(host,user, password, logFile, logLevel)
    userMgr.run(database)


if __name__ == '__main__':

    main()
