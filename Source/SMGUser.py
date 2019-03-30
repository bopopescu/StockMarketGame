

class SMGUser(object):

    def __init__(self, userId, userName, password, fullName, email):
        self.UserId = userId
        self.UserName = userName
        self.FullName = fullName
        self.Password = password
        self.Email = email

    def __str__(self):
        return "%d,%s,%s,%s,%s" % (self.UserId, self.UserName, self.Password,self.FullName, self.Email)

