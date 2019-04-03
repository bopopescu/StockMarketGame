from Source.UserManager import UserManager


class BankManager(object):

    def __init__(self,host, user, password, logger):

        self.UserMgr = UserManager(host, user, password, logger)

    def setLogger(self, logger):

        self.Logger = logger

    def connect(self, database):

        self.UserMgr.connect(database)

    def getPositionValue(self, userId, symbol):

        position = self.UserMgr.getPosition(userId, symbol)

        if position is None:
            return 0

        return position.Amount

    def getPosition(self, userId, symbol):

        return self.UserMgr.getPosition(userId, symbol)

    def getPortfolioValue(self,userId):

        portfolio =self.UserMgr.getPortfolio(userId)

        if portfolio is None:
            return 0

        return portfolio.Amount

    def canTradeCrypto(self, userId, symbol, side, amount, price):

        value = amount * price
        currencies = symbol.split('-')
        if len(currencies) != 2:
            self.Logger.error("Symbol si not crypto symbol.  Format CCY1-CCY2.  Symbol " + symbol)
            return False

        if side == "Buy":
            position = self.getPositionValue(userId,currencies[1])
            if position < value:
                return False
        else:
            position = self.getPositionValue(userId,currencies[0])
            if position < amount:
                return False

        return True

    def canTrade(self, userId, symbol, side, amount, price, secType):
        if secType == "CRYPTO":
            return self.canTradeCrypto(userId, symbol, side, amount, price)
        else:
            self.Logger.error("Security Type not supported - " + secType)
            return False

    def updateCryptoPosition(self, userId, symbol, amount, price, side):

        value = amount * price
        currencies = symbol.split('-')
        if len(currencies) != 2:
            self.Logger.error("Symbol si not crypto symbol.  Format CCY1-CCY2.  Symbol " + symbol)
            return None, None

        buyPosition = sellPosition = None

        if side == "Buy":
            buyPositionValue = float(self.getPositionValue(userId, currencies[0])) + amount
            buyPosition = self.UserMgr.updatePosition(userId, currencies[0], buyPositionValue)
            sellPositionValue = float(self.getPositionValue(userId,currencies[1])) - value
            sellPosition = self.UserMgr.updatePosition(userId, currencies[1], sellPositionValue)
        else:
            sellPositionValue = float(self.getPositionValue(userId,currencies[0])) - amount
            sellPosition = self.UserMgr.updatePosition(userId, currencies[0], sellPositionValue)
            buyPositionValue = float(self.getPositionValue(userId, currencies[1])) + value
            buyPosition = self.UserMgr.updatePosition(userId, currencies[1], buyPositionValue)

        return buyPosition, sellPosition

    def updatePosition(self, userId, symbol, amount, price, side, secType):

        if secType == "CRYPTO":
            return self.updateCryptoPosition(userId, symbol, amount, price, side)
        else:
            self.Logger.error("Symbol si not crypto symbol.  Format CCY1-CCY2.  Symbol " + symbol)
            return None, None
