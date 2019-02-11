import threading, logging, time
from kafka import KafkaConsumer
import binascii
import mysql.connector


class DBWriter(object):

    def __init__(self, host, user, password):
def publishUpdate(mydb, message):
    temp = message.split(',')
    mycursor = mydb.cursor()
    bid = float(temp[2])
    offer = float(temp[3])
    sqlString ="update cryptotopofbook set sequenceno=%s,bestbid=%.2f,bestoffer=%.2f where symbol='%s'" % (temp[0],bid,offer, temp[1])
    mycursor.execute(sqlString)
    mydb.commit()

def main():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="Test"
    )

    print(mydb)
    mycursor = mydb.cursor()
    mycursor.execute("use StockMarketGame")
    mydb.commit()

    consumer = KafkaConsumer(bootstrap_servers='localhost:9092',

                             auto_offset_reset='earliest',

                             consumer_timeout_ms=1000)

    consumer.subscribe(['GDAXFeed'])
    val = 1
    while val == 1:
        for message in consumer:
            msg = message[6].decode("utf-8")
            if "," in msg:
                print(msg)
                publishUpdate(mydb, msg)


if __name__ == '__main__':
    main()
