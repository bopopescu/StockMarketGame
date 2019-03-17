import logging
from Source.StockMarketDB import StockMarketDB


class OrderSequenceQuery(object):

    def __init__(self, host, user, passwd, database):
        self.Host = host
        self.User = user
        self.Password = passwd
        self.Database = database
        self.Db = None

    def __enter__(self):
        self.Db = StockMarketDB(self.User, self.Password, self.Host)
        self.Db.connect()
        self.Db.changeDb(self.Database)

    def getExtOrderSeq(self, system):

        try:
            sqlString = "select max(lastupdate) from smgorder where ordersystem = '%s'" % system
            results = self.DB.select(sqlString)
            if len(results) == 0:
                return 0

            lastupdate = ""
            for result in results:
                lastupdate = result[0]
            sqlString = "select extorderId from smgorder where ordersystem = '%s' and lastupdate = '%s'" \
                        % (system, lastupdate)

            results = self.Db.select(sqlString)
            if len(results) == 0:
                logging.INFO("Did not get back a extorderId for %s.  Strange!!!" % system)
                return 0

            extOrderId = ""
            for result in results:
                extOrderId = result[0]

            temp = extOrderId.split('-')
            if len(temp) != 2:
                logging.INFO("Error trying to split extorderId.  orderId is " + extOrderId)
                return 0

            return int(temp[1])
        except Exception:
            logging.ERROR("Error getting starting ExtOrderId")
            return 0

    def getOrderSeq(self, system):
        try:
            sqlString = "select max(lastupdate) from smgorder where ordersystem = '%s'" % system
            results = self.DB.select(sqlString)
            if len(results) == 0:
                return 0

            lastupdate = ""
            for result in results:
                lastupdate = result[0]
            sqlString = "select orderId from smgorder where ordersystem = '%s' and lastupdate = '%s'" % (system,lastupdate)

            results = self.Db.select(sqlString)
            if len(results) == 0:
                logging.INFO("Did not get back a orderId for %s.  Strange!!!" % system)
                return 0

            orderId = ""
            for result in results:
                orderId = result[0]

            temp = orderId.split('-')
            if len(temp) != 2:
                logging.INFO("Error trying to split OrderId.  orderId is " + orderId)
                return 0

            return int(temp[1])
        except Exception:
            logging.ERROR("Error getting starting OrderId")
            return 0

    def getFillSeq(self,system):

        try:
            sqlString = "select max(created) from smgfill where ordersystem = '%s'" % system
            results =self.Db.select(sqlString)
            if len(results) == 0:
                return 0

            created = ""
            for result in results:
                created = result[0]
            sqlString = "select fillId from smgfill where ordersystem = '%s' and created ='%s'" % (system, created)
            results = self.Db.select(sqlString)

            if len(results) == 0:
                logging.INFO("Did not get back a fillId for SMGExchange.  Strange!!!")
                return 0

            fillId = ""
            for result in results:
                fillId = result[0]

            temp = fillId.split('-')
            if len(temp) != 2:
                logging.INFO("Error trying to split fillId.  FillId is " + fillId)
                return 0

            return int(temp[1])
        except Exception:
            logging.ERROR("Error getting starting FillId")
            return 0
