from Source.SMGOrder import SMGOrder
from Source.SMGFill import SMGFill
from Source.SMGOrderStates import SMOrderStates


class SMGOrderManager(object):

    def __init__(self, orderIdText, orderCounter, fillCounter, system):

        self.Orders = {}
        self.OrderCounter = orderCounter
        self.FillCounter = fillCounter
        self.OrderIdText = orderIdText
        self.System = system

    def getNextOrderId(self):

        self.OrderCounter += 1
        return self.OrderIdText + "-" + str(self.OrderCounter)

    def getNextFillId(self):

        self.FillCounter += 1
        return self.OrderIdText + "-" + str(self.FillCounter)

    def createOrder(self, parentId, orderId, symbol, side, qty, ordType, limitPrice, tif, extOrderId, extSystem):

        if orderId == "":
            orderId = self.getNextOrderId()

        if parentId == "":
            parentId = orderId

        order = SMGOrder(self.System, orderId, parentId,symbol, side, qty, ordType, limitPrice, tif, extOrderId, extSystem)
        self.Orders[orderId] = order

        return order

    def createOrderFromMsg(self, orderMsg):

        temp = orderMsg.split(',')
        if len(temp) != 16:
            return None

        extOrderId = temp[0]
        symbol = temp[1]
        side = temp[2]
        qty = float(temp[3])
        ordType = int(temp[4])
        limitPrice = float(temp[5])
        tif = temp[6]
        extSystem = temp[13]

        order = self.createOrder("","", symbol,side, qty, ordType, limitPrice, tif, extOrderId, extSystem)

        return order

    def createFillFromMsg(self, fillMsg):

        temp = fillMsg.split(',')
        if len(temp) != 8:
            return None

        extFillId = temp[2]
        qty = float(temp[3])
        price = float(temp[4])
        refId = temp[6]
        refTime = temp[5]

        fill = self.createFill("", refId, qty, price, extFillId, refTime)

        return fill

    def createFill(self, fillId, orderId, qty, price, exchangeId, exchangeTime):

        if fillId == "":
            fillId = self.getNextFillId()

        fill = SMGFill(self.System, orderId, fillId, qty, price, exchangeId, exchangeTime)
        if orderId not in self.Orders.keys():
            print("Can't find order for OrderId " + orderId)
            return None
        order = self.Orders[orderId]

        if not order.addFill(fill):
            return None

        if order.OrderId != order.ParentOrderId:
            if order.ParentOrderId not in self.Orders.keys():
                print("Can't find parent Order to update - OrderId " + order.ParentOrderId)
                return None

            parentOrder = self.Orders[order.ParentOrderId]
            parentFill = SMGFill(self.System, parentOrder.OrderId, fillId, qty, price, exchangeId, exchangeTime)

            if not parentOrder.addFill(parentFill):
                return None

        return fill

    def getOrder(self, orderId):

        if orderId not in self.Orders.keys():
            print("Can't find order.  OrderId " + orderId)
            return None

        order = self.Orders[orderId]

        return order

    def cancelOrder(self, orderId):

        if orderId not in self.Orders.keys():
            print("Can't cancel order because I can't find it.  OrderId " + orderId)
            return None

        order = self.Orders[orderId]

        order.updateState(SMOrderStates.PendingCancel)

        return order

    def cancelledOrder(self, orderId):

        if orderId not in self.Orders.keys():
            print("Can't put order in cancelled state because I can't find it.  OrderId " + orderId)
            return None

        order = self.Orders[orderId]

        order.updateState(SMOrderStates.Cancelled)

        return order

    def rejectOrder(self, orderId):

        if orderId not in self.Orders.keys():
            print("Can't reject order because I can't find it.  OrderId " + orderId)
            return None

        order = self.Orders[orderId]

        order.updateState(SMOrderStates.Rejected)

        return order
