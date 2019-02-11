from kafka import KafkaConsumer
import mysql.connector
import sys


class DBWriter(object):

    def __init__(self, host, user, password):

        self.Host = host
        self.User = user
        self.Password = password
        self.Db = None
        self.StartSeq = {}

    def connectToDb(self):
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

    def getKafkaConsumer(self):

        consumer = KafkaConsumer(bootstrap_servers='localhost:9092',

                                 auto_offset_reset='earliest',

                                 consumer_timeout_ms=1000)
        return consumer

    def publishUpdate(self, message):
        temp = message.split(',')
        seq = int(temp[0])
        symbol = temp[1]
        if(seq <= self.StartSeq[symbol]):
            return

        mycursor = self.Db.cursor()
        bid = float(temp[2])
        offer = float(temp[3])
        sqlString ="update cryptotopofbook set sequenceno=%s,bestbid=%.2f,bestoffer=%.2f where symbol='%s'" % (temp[0],bid,offer, temp[1])
        mycursor.execute(sqlString)
        self.Db.commit()

        print(message)

    def getStartSequences(self):

        sqlString = "select symbol, sequenceno from cryptotopofbook"
        mycursor = self.Db.cursor();
        mycursor.execute(sqlString)
        results = mycursor.fetchall()

        for result in results:
            self.StartSeq[result[0]] = int(result[1])


    def run(self, database):
        self.connectToDb()
        self.changeDb(database)
        consumer = self.getKafkaConsumer()

        consumer.subscribe(['GDAXFeed'])

        self.getStartSequences()

        while 1:
            for message in consumer:
                msg = message[6].decode("utf-8")
                if "," in msg:
                    self.publishUpdate(msg)


def main():

    if len(sys.argv) < 5:
        print("usage: DbWriter <host> <user> <password> <database>")
        exit(1)

    writer = DBWriter(sys.argv[1], sys.argv[2], sys.argv[3])
    writer.run(sys.argv[4])


if __name__ == '__main__':
    main()
