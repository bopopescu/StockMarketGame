from websocket import create_connection
import json
from datetime import datetime
import sys
from kafka import KafkaProducer
from Source.SMGConfigMgr import SMGConfigMgr


class GDAXFeedHandler(object):

    def __init__(self, connectionName, tickerFileName):

        self.ConnectionName = connectionName
        self.TickerFileName = tickerFileName
        self.Tickers = []
        self.Producer = KafkaProducer(bootstrap_servers='localhost:9092')

    def getTickers(self):

        fp = open(self.TickerFileName,"r");
        for ticker in fp:
            self.Tickers.append(ticker.strip('\n'))
        fp.close()

    def getSubscriptionString(self):

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

    def subscribe(self, ws):

        self.getTickers()
        subscriptionString = self.getSubscriptionString()
        print("Sending Subscription TO cointbase: " + subscriptionString)
        ws.send(subscriptionString)
        print("Sent subscription")

    def processEvent(self, data):

        f = "%Y-%m-%dT%H:%M:%S.%fZ"
        out = datetime.strptime(data['time'], f)

        output = str(data['sequence']) + "," + data['product_id'] + "," + data['best_bid'] + "," + data[
            'best_ask'] + "," + str(out)

        print("Data Received - %s - Publish to Kafka" % output)
        self.Producer.send('GDAXFeed', output.encode('utf-8'))

    def isHeartbeeatOk(self, heartbeattime):

        current = datetime.now()

        if current.hour < heartbeattime.hour:
            return True

        curval = current.second + (current.minute * 60) + (current.hour * 60 * 60)
        heartval = heartbeattime.second + (heartbeattime.minute * 60) + (heartbeattime.hour * 60 * 60) + 60

        if curval > heartval:
            return False

        return True

    def connectAndSubscribe(self):

        print("connecting to GDAX Exchange to get Market Data")
        ws = create_connection(self.ConnectionName)
        print("Subscribing to data")
        self.subscribe(ws)
        return ws

    def run(self):

        ws = self.connectAndSubscribe()

        print("Receiving Data...")

        heartbeatTime = datetime.now()
        while 1:
            result = ws.recv()
            value = json.loads(result)
            if value['type'] == "ticker" and 'time' in value:
                self.processEvent(value)
            elif value['type'] == "heartbeat":
                heartbeatTime = datetime.now()

            if not self.isHeartbeeatOk(heartbeatTime):
                print("Stale heartbeat. Need to reconnect and subscribe")
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

    if connection is None or tickerFile is None:
        print("Invalid configuration.  Please check")
        exit(1)

    test = GDAXFeedHandler(connection, tickerFile)
    test.run()


if __name__ == '__main__':

    main()
