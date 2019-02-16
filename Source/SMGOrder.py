import datetime
from Source.SMGOrderStates import SMOrderStates


class SMGOrder(object):

    def __init__(self, system, parentOrderId, orderId, symbol, side, qty, ordType, limitPrice, tif, extOrderId, extSystem):
        self.System = system
        self.ParentOrderId = parentOrderId
        self.OrderId = orderId
        self.Symbol = symbol
        self.Side = side
        self.Qty = qty
        self.OrdType = ordType
        self.LimitPrice = limitPrice
        self.TIF = tif
        self.Done = 0
        self.Open = qty
        self.Price = 0.0
        self.Fills = {}
        self.Created = datetime.datetime.now()
        self.LastUpdate = datetime.datetime.now()
        self.State = SMOrderStates.Open.value
        self.ExtOrderId = extOrderId
        self.ExtSystem = extSystem

    def updateFillState(self):

        if self.Qty > self.Done:
            self.State = SMOrderStates.Partial.value
        else:
            self.State = SMOrderStates.Filled.value

    def addFill(self, fill):

        if fill.Qty > self.Open:
            print("Not enough quantity to fill")
            return False
        self.Done += fill.Qty
        self.Open = self.Qty - self.Done
        total = (self.Done * self.Price) + (fill.Qty * fill.Price)
        self.Price = total/self.Done

        self.Fills[fill.FillId] = fill
        self.updateFillState()

        self.LastUpdate = datetime.datetime.now()

        return True

    def updateState(self, state):

        self.State = state,

        self.LastUpdate = datetime.datetime.now()

    def __str__(self):
        return self.OrderId + "," + self.Symbol + "," + self.Side + "," + str(self.Qty) + "," + str(self.OrdType) \
                 + "," + str(self.LimitPrice) + "," + str(self.TIF) + "," + str(self.Done) + "," + str(self.Price) \
                 + "," + str(self.Open) + "," + str(self.Created) + "," + str(self.LastUpdate) + "," + str(self.State) \
                 + "," + self.System + "," + self.ExtOrderId + "," + self.ExtSystem
