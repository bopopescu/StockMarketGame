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

    def updateOrder(self,order):

        sqlString = "update smgorder set doneqty=%.2f,openqty=%.2f" % (order.Done, order.Open)
        sqlString += ",price=%.2f,lastupdate='%s',state=%d" % (order.Price, order.LastUpdate, order.State)
        sqlString += ",extorderid='%s',extsystem='%s' where orderid='%s'" % (order.ExtOrderId, order.ExtSystem, order.OrderId)
        self.Db.update(sqlString)

    def saveNewFill(self,fill):

        sqlString = "insert into smgfill (ordersystem, orderid, fillid,qty,price,created,refid)"
        sqlString += " values('%s','%s','%s', %.2f" % (fill.System, fill.OrderId, fill.FillId, fill.Qty)
        sqlString += ", %.2f, '%s', '%s')" % (fill.Price, fill.Created, fill.RefId)
        self.Db.update(sqlString)
