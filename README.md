This application is a game to teach people about markets.  It is called the Stock Market game but encompasses many different types of instruments.  The first one I will start with is crypto currency.  One of the reasons fo this is that it is easy to get real-time data from the exchanges.  Stock data you need to go make calls to websites.  I will work on that soon. So far there are four main modules.  GDAXFeedHandler (get last trade information from GDAX.  I did not want to process entire books.  I just need best bid and offer.), DBWriter (This writes top of book messages to the database with the last sequence number that I published), SMGExchange (This is the one that will execute trades.  It gets market data and  orders sent to it.  It will also save orders and fills to database), and the last component so far is SMGSimulator (This generates orders on a timer and does a round robin on five different crypto currency products).  All messaging is using Kafka.  I find this easy to use since all you really need to do is publish and subscribe.  The database behind this is MySql. Any questions you can email me at estigum@gmail.com