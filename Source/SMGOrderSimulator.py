from kafka import KafkaProducer
from kafka import KafkaConsumer
from Source.SMGOrderManager import SMGOrderManager
from Source.SMGOrderTypes import SMOrderTypes
import threading


class SMGOrderSimulator(object):

    def __init__(self):

        self.Producer = KafkaProducer(bootstrap_servers='localhost:9092')
        self.Consumer = KafkaConsumer(bootstrap_servers='localhost:9092', auto_offset_reset='earliest', consumer_timeout_ms=1000)
        self.Timer = threading.Timer(10, self.sendOrder)
        self.OM = SMGOrderManager("SIM", 1, 1, "Simulator")
        self.Side = "Sell"

    def setSide(self):

        if self.Side == "Buy":
            self.Side = "Sell"
        else:
            self.Side = "Buy"

    def sendOrder(self):

        self.setSide()
        order = self.OM.createOrder("","","BTC-USD",self.Side,100,SMOrderTypes.Market.value, 0, "Day","","")
        print("Sending Order - " + str(order))
        self.Producer.send('SMGExchangeOrder', str(order).encode('utf-8'))
        self.Timer = threading.Timer(10, self.sendOrder)
        self.Timer.start()

    def run(self):

        self.Consumer.subscribe(['SimulatorFill'])
        self.Timer.start()

        while 1:
            for message in self.Consumer:
                msg = message[6].decode("utf-8")
                print("Got Execution -" + msg)

def main():

    simulator = SMGOrderSimulator()
    simulator.run()


if __name__ == '__main__':

    main()
