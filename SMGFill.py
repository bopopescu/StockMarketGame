import datetime


class SMGFill(object):

    def __init__(self, fillId, qty, price, refId, refTime):
        self.FillId = fillId
        self.Qty = qty
        self.Price = price
        self.RefId = refId
        self.RefTime = refTime
        self.Created = datetime.datetime.now()

    def __str__(self):
        return self.FillId +"," + str(self.Qty) + "," + str(self.Price) +"," + str(self.Created) \
               + "," + self.RefId + "," + str(self.RefTime)
