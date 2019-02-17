import configparser


class SMGConfigMgr(object):

    def __init__(self):

        self.Config = configparser.ConfigParser()
        self.Data = {}

    def load(self,filename):

        self.Config.read(filename)
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
