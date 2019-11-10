import configparser
import os


class SMGConfigMgr(object):

    def __init__(self):

        self.Config = configparser.ConfigParser()
        self.Data = {}
        self.Host = None
        self.User = None
        self.Password = None
        self.Database = None
        self.LogFile = None
        self.LogLevel = None
        self.TickerFile = None
        self.Connection = None

    def load(self, filename):

        configPath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'Configuration'))
        configFilename = configPath

        if os.name == "nt":
            configFilename += "\\" + filename
        else:
            configFilename += "/" + filename

        self.Config.read(configFilename)
        sections = self.Config.sections()
        for section in sections:
            optionData = {}
            options = self.Config.options(section)
            for option in options:
                optionData[option] = self.Config.get(section, option)
            self.Data[section] = optionData

    def getConfigItem(self, section, option):

        if section not in self.Data:
            print("Can't find section " + section)
            return None

        optionData = self.Data[section]

        if option not in optionData:
            print("Can't find option " + option)
            return None

        return optionData[option]

    def setConfigItems(self):

        if "DatabaseInfo" in self.Data:
            self.Host = self.getConfigItem("DatabaseInfo", "host")
            self.User = self.getConfigItem("DatabaseInfo", "user")
            self.Password = self.getConfigItem("DatabaseInfo", "passwd")
            self.Database = self.getConfigItem("DatabaseInfo", "db")

        if "Logging" in self.Data:
            self.LogFile = self.getConfigItem("Logging", "filename")
            self.LogLevel = self.getConfigItem("Logging", "loglevel")

        if "FeedHandler" in self.Data:
            self.TickerFile = self.getConfigItem("FeedHandler", "tickerfile")
            self.Connection = self.getConfigItem("FeedHandler", "connection")


