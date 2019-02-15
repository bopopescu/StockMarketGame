CREATE TABLE cryptotopofbook
(
    sequenceno bigint,
    symbol varchar(30) NOT NULL,
    bestbid decimal(15,6),
    bestoffer decimal(15,6),
    timestamp datetime,
    PRIMARY KEY (symbol)
);
insert into cryptotopofbook (sequenceno, symbol, bestbid, bestoffer,timestamp) values(1,'BTC-USD',0,0,'2019-02-14 16:00:10.726000');
insert into cryptotopofbook (sequenceno, symbol, bestbid, bestoffer,timestamp) values(1,'BCH-USD',0,0,'2019-02-14 16:00:10.726000');
insert into cryptotopofbook (sequenceno, symbol, bestbid, bestoffer,timestamp) values(1,'LTC-USD',0,0,'2019-02-14 16:00:10.726000');
insert into cryptotopofbook (sequenceno, symbol, bestbid, bestoffer,timestamp) values(1,'ETH-USD',0,0,'2019-02-14 16:00:10.726000');
insert into cryptotopofbook (sequenceno, symbol, bestbid, bestoffer,timestamp) values(1,'ZRX-USD',0,0,'2019-02-14 16:00:10.726000');
