

class SMGPosition(object):

    def __init__(self, userId, symbol, amount, created, lastUpdate ):

        self.UserId = userId
        self.Symbol = symbol
        self.Amount = amount
        self.Created = created
        self.LastUpdate = lastUpdate

    def __str__(self):

        return "%d,%s,%18.6f,%s,%s" % (self.UserId, self.Symbol, self.Amount, self.Created, self.LastUpdate)
