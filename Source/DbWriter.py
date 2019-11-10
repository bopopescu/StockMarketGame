from kafka import KafkaConsumer
import sys
from Source.StockMarketDB import StockMarketDB
from Source.SMGConfigMgr import SMGConfigMgr
from Source.SMGLogger import SMGLogger
from Source.KafkaAdminMgr import KafkaAdminMgr


class DBWriter(object):

    def __init__(self, host, user, password, logFile, logLevel):

        self.Db = StockMarketDB(user, password, host)
        self.StartSeq = {}
        self.Logger = SMGLogger(logFile, logLevel)
        self.KafkaAdmin = KafkaAdminMgr()


    def getKafkaConsumer(self):

        consumer = KafkaConsumer(bootstrap_servers='localhost:9092',

                                 auto_offset_reset='earliest',

                                 consumer_timeout_ms=1000)
        return consumer

    def publishUpdate(self, message):

        try:
            temp = message.split(',')
            seq = int(temp[0])
            symbol = temp[1]

            if symbol in self.StartSeq:
                if seq <= self.StartSeq[symbol]:
                    return

            bid = float(temp[2])
            offer = float(temp[3])
            timestamp = temp[4]

            sqlString = "update cryptotopofbook set sequenceno=%d,bestbid=%.8f,bestoffer=%.8f," \
                        "timestamp='%s' where symbol='%s'" % (seq, bid, offer, timestamp, symbol)

            self.Db.update(sqlString)
            self.Logger.info(sqlString)
        except Exception:
            self.Logger.error("Error publishing update - " + message)

    def getStartSequences(self):

        sqlString = "select symbol, sequenceno from cryptotopofbook"
        results = self.Db.select(sqlString)
        for result in results:
            self.StartSeq[result[0]] = int(result[1])

    def run(self, database):

        self.Db.connect()
        self.Db.changeDb(database)
        consumer = self.getKafkaConsumer()

        self.Logger.info("Subscribe to GDAXFeed")
        self.KafkaAdmin.addTopics(['GDAXFeed'])
        consumer.subscribe(['GDAXFeed'])

        self.getStartSequences()

        while 1:
            for message in consumer:
                msg = message[6].decode("utf-8")
                if "," in msg:
                    self.publishUpdate(msg)


def main():
    if len(sys.argv) != 2:
        print("usage: DbWriter.py <configfile>")
        exit(1)

    config = SMGConfigMgr()
    config.load(sys.argv[1])
    config.setConfigItems()

    if config.Host is None or config.User is None or config.Password is None or config.Database is None or config.LogFile is None or config.LogLevel is None:
        print("Invalid configuration items.  Please check config file.")
        exit(1)

    writer = DBWriter(config.Host, config.User, config.Password, config.LogFile, config.LogLevel)
    writer.run(config.Database)


if __name__ == '__main__':

    main()
