

class SMGPortfolio(object):

    def __init__(self, userId, amount, created, lastUpdate):

        self.UserId = userId
        self.Amount = amount
        self.Created = created
        self.LastUpdate = lastUpdate

    def __str__(self):

        return "%d,%18.6f,%s,%s" % (self.UserId, self.Amount, self.Created, self.LastUpdate)
