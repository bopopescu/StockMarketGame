from SMGOrder import SMGOrder
from SMGFill import SMGFill
from SMGOrderStates import SMOrderStates


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
        if len(temp) == 1:
            return
        orderId = temp[0]
        symbol = temp[1]
        side = temp[2]
        qty = float(temp[3])
        ordType = temp[4]
        limitPrice = float(temp[5])
        tif = temp[6]
        extSystem = ""
        if len(temp) == 16:
            extSystem = temp[13]

        order = self.createOrder("","", symbol,side, qty, ordType, limitPrice, tif, orderId, extSystem)

        return order

    def createFill(self, fillId, orderId, qty, price, exchangeId, exchangeTime):

        if fillId == "":
            fillId = self.getNextFillId()

        fill = SMGFill(fillId, qty, price, exchangeId, exchangeTime);

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

            if not parentOrder.addFill(fill):
                return None

        return fill

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
