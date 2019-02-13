from kafka import KafkaProducer
from kafka import KafkaConsumer
from SMGOrderManager import SMGOrderManager
import datetime

class SMGExchange(object):

    def __init__(self):

        self.Orders = {}
        self.Fills = {}
        self.Bids = {}
        self.Offers = {}
        self.OM = SMGOrderManager("EXCH",1,1,"SMGExchange")
        self.Producer = KafkaProducer(bootstrap_servers='localhost:9092')
        self.Consumer = KafkaConsumer(bootstrap_servers='localhost:9092', auto_offset_reset='earliest', consumer_timeout_ms=1000)

    def processBidOffer(self,message):

        temp = message.split(',')
        if len(temp) == 1:
            return

        symbol = temp[1]
        bid = float(temp[2])
        offer = float(temp[3])
        self.Bids[symbol] = bid
        self.Offers[symbol] = offer
        print("Update Bid/Offer for " + symbol + " " + str(bid) + " X " + str(offer))

    def getPrice(self, symbol, side):

        if side == "Buy":
            if symbol in self.Offers.keys():
                return self.Offers[symbol]
        else:
            if symbol in self.Bids.keys():
                return self.Bids[symbol]

        return 1

    def processOrder(self,message):

        order = self.OM.createOrderFromMsg(message)
        price = self.getPrice(order.Symbol, order.Side)
        fill = self.OM.createFill("", order.OrderId,order.Qty, price, order.ExtOrderId, datetime.datetime.now())
        topic = order.ExtSystem + "Fill"
        print("Sending fill - Topic " + topic + " - " + str(fill))
        self.Producer.send(topic, str(fill).encode('utf-8'))

    def run(self):

        self.Consumer.subscribe(['GDAXFeed', 'SMGExchangeOrder'])

        while 1:
            for message in self.Consumer:
                msg = message[6].decode("utf-8")
                if message[0] == "GDAXFeed":
                    self.processBidOffer(msg)
                elif message[0] == "SMGExchangeOrder":
                    print("Got an order - " + msg)
                    self.processOrder(msg)


def main():
    client = SMGExchange()
    client.run()


if __name__ == '__main__':

    main()

