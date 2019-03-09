from Source.StockMarketDB import StockMarketDB
import datetime
from Source.SMGUser import SMGUser
from Source.SMGPortfolio import SMGPortfolio
from Source.SMGPosition import SMGPosition

class UserManager(object):

    def __init__(self, host, user, password, logger):

        self.Db = StockMarketDB(user, password, host)
        self.Loggger = logger
        self.Users = {}
        self.UsersById = {}
        self.Portfolios = {}
        self.Positions = {}
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

    def createPortfolioFromDbRecord(self, record):

        userId = record[0]
        amount = record[1]
        created = record[2]
        lastUpdate = record[3]

        portfolio = SMGPortfolio(userId, amount, created, lastUpdate)
        return portfolio

    def createPositionFromDbRecord(self, record):

        userId = record[0]
        symbol = record[1]
        amount = record[2]
        created = record[3]
        lastUpdate = record[4]

        position = SMGPosition(userId, symbol, amount, created, lastUpdate)
        return position

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
            self.UsersById[user.UserId] = user
            return user

        return None

    def getPortfolio(self, userId):

        if userId in self.Portfolios:
            return self.Portfolios[userId]

        sqlString = "select * from smgportfolio where userid =%d" % (userId)
        results = self.Db.select(sqlString)
        for result in results:
            portfolio = self.createPortfolioFromDbRecord(result)
            self.Portfolios[userId] = portfolio
            return portfolio

        return None

    def getPosition(self, userId, symbol):

        if userId in self.Positions:
            symbolList = self.Positions[userId]
            if symbol in symbolList:
                return symbolList[symbol]

        sqlString = "select * from smgposition where userid =%d and symbol='%s'" % (userId, symbol)
        results = self.Db.select(sqlString)
        for result in results:
            position = self.createPositionFromDbRecord(result)
            if userId in self.Positions:
                symbolList = self.Positions[userId]
                symbolList[symbol] = position
            else:
                symbolList = {}
                symbolList[symbol] = position
                self.Positions[userId] = symbolList
            return position

        return None

    def saveUser(self, user):

        sqlString ="insert into smguser (username,password,fullname,email) values('%s', '%s', '%s', '%s')" % (user.UserName, user.Password, user.FullName, user.Email)

        self.Db.update(sqlString)

    def updateUserHistory(self,user, status):

        lastupdate = datetime.datetime.now()
        sqlString = "insert into smguserhistory (userid, lastupdate, status) values (%d,'%s','%s')" % (user.UserId, lastupdate, status)

        self.Db.update(sqlString)

    def updatePosition(self, userId, symbol, amount):

        lastupdate = datetime.datetime.now()
        sqlString = "update smgposition set amount = %18.6f and lastupdate = '%s' where userid = %d and symbol = '%s'" \
            % (amount, lastupdate, userId, symbol)

        self.Db.update(sqlString)
        position = self.getPosition(userId, symbol)
        if position is None:
            self.Loggger.Error("Can't find position to update. Symbol " + symbol)
            return None

        position.Amount = amount

        return position

    def createPortfolio(self, user, amount):

        lastupdate = datetime.datetime.now()
        sqlString = "insert into smgportfolio (userid, amount, created, lastupdate) values(%d, %16.8f, '%s', '%s')" % (user.UserId, amount, lastupdate, lastupdate)

        self.Db.update(sqlString)

    def createInitialPosition(self, user, currency, amount):

        lastupdate = datetime.datetime.now()
        sqlString = "insert into smgposition (userid, symbol, amount, created, lastupdate) values (%d, '%s', %16.8f, '%s', '%s')" % (user.UserId, currency, amount, lastupdate, lastupdate)

        self.Db.update(sqlString)

    def loadUsers(self):

        sqlString = "select * from smguser"
        results = self.Db.select(sqlString)

        for result in results:
            user = self.createUserObjectFromDbRecord(result)
            self.Users[user.UserName] = user
            self.UsersById[user.UserId] = user
            if user.UserId > self.MaxUserId:
                self.MaxUserId = user.UserId

    def loadPortfolios(self):

        results = self.Db.select("select * from smgportfolio")
        for result in results:
            portfolio = self.createPortfolioFromDbRecord(result)
            self.Portfolios[portfolio.UserId] = portfolio

    def loadPositions(self):

        results = self.Db.select("select * from smgposition")
        for result in results:
            position = self.createPositionFromDbRecord(result)
            if position.UserId in self.Positions:
                symList = self.Positions[position.UserId]
                symList[position.Symbol] = position
            else:
                symList = {}
                symList[position.Symbol] = position
                self.Positions[position.UserId] = symList

    def doesUserExist(self,username):

        if username in self.Users:
            return True

        return False
