from Source.StockMarketDB import StockMarketDB
import datetime
from Source.SMGUser import SMGUser

class UserManager(object):

    def __init__(self, host, user, password, logger):

        self.Db = StockMarketDB(user, password, host)
        self.Loggger = logger
        self.Users = {}
        self.MaxUserId = 0

    def connect(self, database):

        self.Db.connect()
        self.Db.changeDb(database)

    def createUserObjectFromDbRecord(self, record):

        userId = record[0]
        username = record[1]
        password = record[2]
        fullname = record[3]
        email = record[4]

        user = SMGUser(userId, username, password, fullname, email)
        return user

    def createUserObjectFromMessage(self, userMsg):

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

        if username in self.Users:
            return self.Users[username]

        sqlString = "select * from smguser where username ='%s'" % (username)
        results = self.Db.select(sqlString)
        for result in results:
            user = self.createUserObjectFromDbRecord(result)
            self.Users[user.UserName] = user
            return user

        return None

    def saveUser(self, user):

        sqlString ="insert into smguser (username,password,fullname,email) values('%s', '%s', '%s', '%s')" % (user.UserName, user.Password, user.FullName, user.Email)

        self.Db.update(sqlString)

    def updateUserHistory(self,user, status):

        lastupdate = datetime.datetime.now()
        sqlString = "insert into smguserhistory (userid, lastupdate, status) values (%d,'%s','%s')" % (user.UserId, lastupdate, status)

        self.Db.update(sqlString)

    def createPortfolio(self, user, amount):

        lastupdate = datetime.datetime.now()
        sqlString = "insert into smgportfolio (userid, amount, created, lastupdate) values(%d, %16.8f, '%s', '%s')" % (user.UserId, amount, lastupdate, lastupdate)

        self.Db.update(sqlString)

    def createInitialPosition(self, user, currency, amount):

        lastupdate = datetime.datetime.now()
        sqlString = "insert into smgposition (userid, symbol, amount, created, lastupdate) values (%d, '%s', %16.8f, '%s', '%s')" % (user.UserId, currency, amount, lastupdate, lastupdate)

        self.Db.update(sqlString)

    def loadExistingUsers(self):

        sqlString = "select * from smguser"
        results = self.Db.select(sqlString)

        for result in results:
            user = self.createUserObjectFromDbRecord(result)
            self.Users[user.UserName] = user
            if user.UserId > self.MaxUserId:
                self.MaxUserId = user.UserId

    def doesUserExist(self,username):

        if username in self.Users:
            return True

        return False
