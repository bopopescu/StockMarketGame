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

    def setFillSeq(self, seq):

        self.FillCounter = seq

    def setOrderSeq(self, seq):

        self.OrderCounter = seq

    def getNextOrderId(self):

        self.OrderCounter += 1
        return self.OrderIdText + "-" + str(self.OrderCounter)

    def getNextFillId(self):

        self.FillCounter += 1
        return self.OrderIdText + "-" + str(self.FillCounter)

    def isValidFill(self, message):

        temp = message.split(',')
        if len(temp) != 9:
            return False

        temp2 = temp[6].split('-')
        if len(temp2) != 2:
            return False

        if int(temp2[1]) <= self.FillCounter:
            return False

        return True

    def createFillFromDbRecord(self, result):

        fill = self.createFill(result[2],result[1], float(result[3]), float(result[4]), result[6], result[5], int(result[7]))

        return fill

    def createOrderFromDbRecord(self, result):

        orderId = result[2]

        order = SMGOrder(result[0],orderId, result[1], result[3],result[4],float(result[5]),int(result[12]), float(result[9]), result[13], result[15], result[16], int(result[17]), result[18])

        self.Orders[orderId] = order

        return order

    def createOrder(self, parentId, orderId, symbol, side, qty, ordType, limitPrice, tif, extOrderId, extSystem, userId, secType):

        if orderId == "":
            orderId = self.getNextOrderId()

        if parentId == "":
            parentId = orderId

        order = SMGOrder(self.System, orderId, parentId,symbol, side, qty, ordType, limitPrice, tif, extOrderId, extSystem, userId, secType)
        self.Orders[orderId] = order

        return order

    def createOrderFromMsg(self, orderMsg, userId):

        temp = orderMsg.split(',')
        if len(temp) != 18:
            return None

        extOrderId = temp[0]
        symbol = temp[1]
        side = temp[2]
        qty = float(temp[3])
        ordType = int(temp[4])
        limitPrice = float(temp[5])
        tif = temp[6]
        extSystem = temp[13]
        secType = temp[17]

        order = self.createOrder("","", symbol,side, qty, ordType, limitPrice, tif, extOrderId, extSystem, userId, secType)

        return order

    def createFillFromMsg(self, fillMsg, userId):

        temp = fillMsg.split(',')
        if len(temp) != 9:
            return None

        extFillId = temp[2]
        qty = float(temp[3])
        price = float(temp[4])
        refId = temp[6]

        refTime = temp[5]

        fill = self.createFill("", refId, qty, price, extFillId, refTime, userId)

        return fill

    def createFill(self, fillId, orderId, qty, price, exchangeId, exchangeTime, userId):

        if fillId == "":
            fillId = self.getNextFillId()

        fill = SMGFill(self.System, orderId, fillId, qty, price, exchangeId, exchangeTime, userId)
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
            parentFill = SMGFill(self.System, parentOrder.OrderId, fillId, qty, price, exchangeId, exchangeTime, userId)

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
