import mysql.connector

class StockMarketDB(object):

    def __init__(self, user, password, host):

        self.Host = host
        self.User = user
        self.Password = password
        self.Db = None

    def connect(self):

        self.Db = mysql.connector.connect(
            host = self.Host,
            user = self.User,
            passwd = self.Password
        )
        print(self.Db)

    def changeDb(self, dbname):

        mycursor = self.Db.cursor()
        mycursor.execute("use " + dbname)
        self.Db.commit()

    def update(self, sqlString):

        mycursor = self.Db.cursor()
        mycursor.execute(sqlString)
        self.Db.commit()

    def select(self, sqlString):

        mycursor = self.Db.cursor();
        mycursor.execute(sqlString)
        return mycursor.fetchall()

