from Source.StockMarketDB import StockMarketDB

class OrderLoader(object):
    def __init__(self, host, user, password, database):

        self.Host = host
        self.User = user
        self.Password = password
        self.Database = database
        self.Db = None

    def __enter__(self):

        self.Db = StockMarketDB(self.User, self.Password, self.Host)
        self.Db.connect()
        self.Db.changeDb(self.Database)

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("Done")

    def loadOrders(self, orderManager, system):

        sqlText = "select * from smgorder where ordersystem = '%s'" % (system)
        results = self.Db.select(sqlText)
        if len(results) == 0:
            return
        for result in results:
            orderManager.createOrderFromDbRecord(result)

    def loadFills(self, orderManager, system):

        sqlText = "select * from smgfill where ordersystem = '%s'" % (system)
        results = self.Db.select(sqlText)
        if len(results) == 0:
            return
        for result in results:
            orderManager.createFillFromDbRecord(result)
