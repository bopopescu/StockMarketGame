from websocket import create_connection
import json
from datetime import datetime
import sys
from kafka import KafkaProducer
from Source.SMGConfigMgr import SMGConfigMgr
from Source.SMGLogger import SMGLogger
import os


class GDAXFeedHandler(object):

    def __init__(self, connectionName, tickerFileName, logFile, logLevel):

        self.ConnectionName = connectionName
        self.TickerFileName = tickerFileName
        self.Tickers = []
        self.Producer = KafkaProducer(bootstrap_servers='localhost:9092')
        self.Logger = SMGLogger(logFile, logLevel)

    def getTickers(self):

        try:
            tickerPath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
            tickerFilename = tickerPath + "\\" + self.TickerFileName

            fp = open(tickerFilename,"r")
            for ticker in fp:
                self.Tickers.append(ticker.strip('\n'))
            fp.close()
        except Exception:
            self.Logger.error("Error processing Ticker FIle - " + tickerFilename)

    def getSubscriptionString(self):

        try:
            connectionString = "{\"type\": \"subscribe\",\"product_ids\": "
            count = 0
            tickerString = "["
            for ticker in self.Tickers:
                if count > 0:
                    tickerString += ","
                tickerString += "\"" + ticker + "\""
                count += 1
            tickerString += "]"
            connectionString += tickerString
            connectionString +=",\"channels\": [\"heartbeat\",{\"name\": \"ticker\",\"product_ids\": "
            connectionString += tickerString
            connectionString += "}]}"
            return connectionString
        except Exception:
            self.Logger.error("Error processing subscription string")

    def subscribe(self, ws):

        try:
            self.getTickers()
            subscriptionString = self.getSubscriptionString()
            self.Logger.info("Sending Subscription TO cointbase: " + subscriptionString)
            ws.send(subscriptionString)
            self.Logger.info("Sent subscription")
        except Exception:
            raise Exception("Error sending subscription")

    def processEvent(self, data):

        try:
            f = "%Y-%m-%dT%H:%M:%S.%fZ"
            out = datetime.strptime(data['time'], f)

            output = str(data['sequence']) + "," + data['product_id'] + "," + data['best_bid'] + "," + data[
                'best_ask'] + "," + str(out)

            self.Logger.info("Data Received - %s - Publish to Kafka" % output)
            self.Producer.send('GDAXFeed', output.encode('utf-8'))
        except Exception:
            self.Logger.error("Error processing event - " + str(data))

    def isHeartbeatOk(self, heartbeattime):

        try:
            current = datetime.now()

            if current.hour < heartbeattime.hour:
                return True

            curval = current.second + (current.minute * 60) + (current.hour * 60 * 60)
            heartval = heartbeattime.second + (heartbeattime.minute * 60) + (heartbeattime.hour * 60 * 60) + 60

            if curval > heartval:
                return False

            return True
        except Exception:
            self.Logger.error("Error checking heartbeat")

    def connectAndSubscribe(self):

        self.Logger.info("connecting to GDAX Exchange to get Market Data")
        ws = create_connection(self.ConnectionName)
        self.Logger.info("Subscribing to data")
        self.subscribe(ws)
        return ws

    def run(self):

        ws = self.connectAndSubscribe()

        self.Logger.info("Receiving Data...")

        heartbeatTime = datetime.now()
        while 1:
            result = ws.recv()
            value = json.loads(result)
            if value['type'] == "ticker" and 'time' in value:
                self.processEvent(value)
            elif value['type'] == "heartbeat":
                heartbeatTime = datetime.now()

            if not self.isHeartbeatOk(heartbeatTime):
                self.Logger.info("Stale heartbeat. Need to reconnect and subscribe")
                ws.close()
                ws = self.connectAndSubscribe()

        ws.close()


def main():

    if len(sys.argv) != 2:
        print("usage: GDAXFeedHandler.py <configfile>")
        exit(1)

    config = SMGConfigMgr()
    config.load(sys.argv[1])

    connection = config.getConfigItem("FeedHandler", "connection")
    tickerFile = config.getConfigItem("FeedHandler", "tickerfile")
    logFile = config.getConfigItem("Logging", "filename")
    logLevel = config.getConfigItem("Logging", "loglevel")

    if connection is None or tickerFile is None or logFile is None or logLevel is None:
        print("Invalid configuration.  Please check")
        exit(1)

    test = GDAXFeedHandler(connection, tickerFile, logFile, logLevel)
    test.Logger.info("Getting Ready to run")
    test.run()


if __name__ == '__main__':

    main()
