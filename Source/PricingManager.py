import logging

class PricingManager(object):

    def __init__(self):
        self.Bids = {}
        self.Offers = {}

    def processPriceMsg(self, message):

        try:
            temp = message.split(',')
            if len(temp) == 1:
                return

            symbol = temp[1]
            self.Bids[symbol] = float(temp[2])
            self.Offers[symbol] = float[temp[3]]
        except Exception:
            logging.ERROR("Error processing Bid/Offer message " + message)

    def getPrice(self, symbol, side):

        if side == "Buy":
            return self.Offers.get(symbol, 0)
        elif side == "Sell":
            return self.Bids.get(symbol, 0)

        logging.WARNING("Not able to find pricer for symbol " + symbol)
        return 0
