from kafka import KafkaConsumer
import sys
from StockMarketDB import StockMarketDB


class DBWriter(object):

    def __init__(self, host, user, password):
        self.Db = StockMarketDB(user, password, host)
        self.StartSeq = {}

    def getKafkaConsumer(self):

        consumer = KafkaConsumer(bootstrap_servers='localhost:9092',

                                 auto_offset_reset='earliest',

                                 consumer_timeout_ms=1000)
        return consumer

    def publishUpdate(self, message):

        temp = message.split(',')
        seq = int(temp[0])
        symbol = temp[1]

        if seq <= self.StartSeq[symbol]:
            return

        bid = float(temp[2])
        offer = float(temp[3])

        sqlString = "update cryptotopofbook set sequenceno=%d,bestbid=%.2f,bestoffer=%.2f where symbol='%s'" % (seq, bid, offer, symbol)
        self.Db.update(sqlString)
        print(message)

    def getStartSequences(self):

        sqlString = "select symbol, sequenceno from cryptotopofbook"
        results = self.Db.select(sqlString)

        for result in results:
            self.StartSeq[result[0]] = int(result[1])

    def run(self, database):

        self.Db.connect()
        self.Db.changeDb(database)
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
