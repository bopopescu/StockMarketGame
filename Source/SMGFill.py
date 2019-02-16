import datetime


class SMGFill(object):

    def __init__(self, system, orderId, fillId, qty, price, refId, refTime):

        self.System = system
        self.OrderId = orderId
        self.FillId = fillId
        self.Qty = qty
        self.Price = price
        self.RefId = refId
        self.RefTime = refTime
        self.Created = datetime.datetime.now()

    def __str__(self):
        return self.System + "," + self.OrderId + "," + self.FillId + "," + str(self.Qty) + "," \
               + str(self.Price) +"," + str(self.Created) \
               + "," + self.RefId + "," + str(self.RefTime)
