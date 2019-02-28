from kafka import KafkaConsumer
from kafka import KafkaProducer
import sys
from Source.StockMarketDB import StockMarketDB
from Source.SMGConfigMgr import SMGConfigMgr
from Source.SMGLogger import SMGLogger
from Source.SMGUser import  SMGUser
import datetime

class SMGUserManager(object):

    def __init__(self, host, user, password, logFile, logLevel):

        self.Db = StockMarketDB(user, password, host)
        self.StartSeq = {}
        self.Logger = SMGLogger(logFile, logLevel)
        self.Users = {}
        self.MaxUserId = 0
        self.Producer = KafkaProducer(bootstrap_servers='localhost:9092')
        self.Consumer = KafkaConsumer(bootstrap_servers='localhost:9092', auto_offset_reset='earliest', consumer_timeout_ms=1000)


    def createUserObjectFromDbRecord(self, record):

        userId = record[0]
        username = record[1]
        password = record[2]
        fullname = record[3]
        email = record[4]

        user = SMGUser(userId, username, password, fullname, email)
        return user

    def createUserObjectFromMessage(userMsg):

        temp = userMsg.split(',')
        if len(temp) != 5:
            return None
        userId = int(temp[0])
        userName = temp[1]
        password = temp[2]
        fullName = temp[3]
        email = temp[4]

        user = SMGUser(userId, userName, password, fullName, email)
        return user

    def getUser(self, username):

        sqlString = "select * from smguser where username ='%s'" % (username)
        results = self.Db.select(sqlString)
        for result in results:
            return self.createUserObjectFromDbRecord(result)

    def saveUser(self, user):

        sqlString ="insert into smguser (username,password,fullname,email) values('%s', '%s', '%s', '%s')" % (user.UserName, user.Password, user.FullName, user.Email)

        self.Db.update(sqlString)

    def updateUserHistory(self,user, status):

        lastupdate = datetime.datetime.now()
        sqlString = "insert into smguserhistory (userid, lastupdate, status) values (%d,'%s','%s')" %(user.UserId, lastupdate, status)

        self.Db.update(sqlString)

    def createPortfolio(self, user, amount):

        lastupdate = datetime.datetime.now()
        sqlString = "insert into smgportfolio (user, amount, created, lastupdate) values(%d, %16.8f, '%s', '%s')" & (user.UserId, amount, lastupdate, lastupdate)

        self.Db.update(sqlString)

    def createInitialPosition(self, user, currency, amount):

        lastupdate = datetime.datetime.now()
        sqlString = "insert into smgposotion (userid, symbol, amount, created, lastupdate) values (%d, '%s', %16.8f, '%s', '%s'" \
                    % (user.UserId, currency, amount, lastupdate, lastupdate)

        self.Db.update(sqlString)

    def loadExistingUsers(self):

        sqlString = "select * from smguser"
        results = self.Db.select(sqlString)

        for result in results:
            user = self.createUserObjectFromDbRecord(result)
            self.Users[user.UserName] = user
            if user.UserId > self.MaxUserId:
                self.MaxUserId = user.UserId

    def run(self, database):
        self.Db.connect()
        self.Db.changeDb(database)
        self.loadExistingUsers()

        self.Logger.info("Subscribe to SMGNewUser")
        self.Consumer.subscribe(['SMGNewUser'])

        self.getStartSequences()

        while 1:
            for message in self.Consumer:
                msg = message[6].decode("utf-8")
                user = self.createUserObjectFromMessage(msg)
                if user.UserName in self.Users:
                    self.Logger.error("User already exist.  Can't create user " + user.UserName)
                    self.Producer.send("UserAddFailed", "User already exists - " + user.UserName)
                else:
                    self.saveUser(user)
                    actuser = self.getUser(user.UserName)
                    if actuser is None:
                        self.Logger.error("Error creating user")
                        self.Producer.send("UserAddFailed", "User did not save " + user.UserName)
                    else:
                        self.updateUserHistory(actuser, "NEW")
                        self.createPortfolio(actuser, 15000000.00)
                        self.createInitialPosition(actuser, "USD", 15000000.00)
                        self.Producer.send("UserAddSucceeded", str(actuser))

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
