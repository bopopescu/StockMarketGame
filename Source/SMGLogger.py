import logging
import os


class SMGLogger(object):

    def __init__(self, logFilename, logLevel):

        loggingPath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'Logging'))
        loggingFilename = loggingPath

        if os.name == "nt":
            loggingFilename += "\\" + logFilename
        else:
            loggingFilename += "/" + logFilename

        loggingLevel = logging.DEBUG
        if logLevel == "INFO":
            loggingLevel = logging.INFO
        elif logLevel == "WARNING":
            loggingLevel = logging.WARNING
        elif logLevel == "ERROR":
            loggingLevel = logging.ERROR

        logging.basicConfig(filename=loggingFilename, level=loggingLevel, format='%(asctime)s %(levelname)s - %(message)s')

    def debug(self, message):

        logging.debug(message)

    def info(self, message):

        logging.info(message)

    def error(self, message):

        logging.error(message)

    def warning(self, message):

        logging.warning(message)
