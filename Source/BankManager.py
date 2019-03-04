from Source.UserManager import UserManager


class BankManager(object):

    def __init__(self,host, user, password, logger):

        self.UserMgr = UserManager(host, user, password, logger)
        self.Logger = logger

    def connect(self, database):

        self.UserMgr.connect(database)
