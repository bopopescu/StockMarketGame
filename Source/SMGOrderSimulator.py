from kafka import KafkaProducer
from kafka import KafkaConsumer
from Source.SMGOrderManager import SMGOrderManager
from Source.SMGOrderTypes import SMOrderTypes
import threading
from Source.DBOrderManagerWriter import DBOrderManagerWriter


class SMGOrderSimulator(object):

    def __init__(self):

        self.Producer = KafkaProducer(bootstrap_servers='localhost:9092')
        self.Consumer = KafkaConsumer(bootstrap_servers='localhost:9092', auto_offset_reset='earliest', consumer_timeout_ms=1000)
        self.Timer = threading.Timer(10, self.sendOrder)
        self.OM = SMGOrderManager("SIM", 24, 24, "Simulator")
        self.Side = "Sell"
        self.DB = DBOrderManagerWriter("localhost", "gdaxuser", "AnimalHouse1010", "StockMarketGame")

    def setSide(self):

        if self.Side == "Buy":
            self.Side = "Sell"
        else:
            self.Side = "Buy"

    def sendOrder(self):

        self.setSide()
        order = self.OM.createOrder("","","BTC-USD",self.Side,100,SMOrderTypes.Market.value, 0, "Day","","")
        self.DB.saveNewOrder(order)
        print("Sending Order - " + str(order))
        self.Producer.send('SMGExchangeOrder', str(order).encode('utf-8'))
        self.Timer = threading.Timer(10, self.sendOrder)
        self.Timer.start()

    def processFill(self, message):

        fill = self.OM.createFillFromMsg(message)
        if fill is None:
            return

        print("Got Execution -" + str(fill))
        self.DB.saveNewFill(fill)

        order = self.OM.getOrder(fill.OrderId)
        if order is None:
            return
        self.DB.updateOrder(order)

    def run(self):

        self.Consumer.subscribe(['SimulatorFill'])
        self.Timer.start()

        while 1:
            for message in self.Consumer:
                msg = message[6].decode("utf-8")
                self.processFill(msg)

def main():

    simulator = SMGOrderSimulator()
    simulator.run()


if __name__ == '__main__':

    main()
