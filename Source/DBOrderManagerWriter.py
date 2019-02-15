from Source.StockMarketDB import StockMarketDB


class DBOrderManagerWriter(object):
    def __init__(self, host, user, password, database):
        self.Db = StockMarketDB(user, password, host)
        self.Db.connect()
        self.Db.changeDb(database)

    def saveNewOrder(self,order):

        sqlString = "insert into smgorder (ordersystem,orderid, parentid,symbol,side,qty,doneqty,openqty"
        sqlString += ",price,limitprice,created,lastupdate,ordtype,tif,state,extorderid,extsystem)"
        sqlString += " values('%s','%s','%s','%s','%s'" % (order.System, order.OrderId, order.ParentOrderId, order.Symbol, order.Side)
        sqlString += ",%.2f,%.2f,%.2f,%.2f,%.2f" % (order.Qty, order.Done, order.Open,order.Price, order.LimitPrice)
        sqlString += ",'%s','%s', %d, '%s'" % (order.Created, order.LastUpdate, order.OrdType, order.TIF)
        sqlString += ",%d,'%s','%s')" % (order.State, order.ExtOrderId, order.ExtSystem)
        self.Db.update(sqlString)
