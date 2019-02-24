use stockmarketgame;

TRUNCATE TABLE cryptotopofbook;
DROP TABLE cryptotopofbook IF EXISTS;

TRUNCATE TABLE smgfill;
DROP TABLE smgfill IF EXISTS;

TRUNCATE TABLE smgorder;
DROP TABLE smgorder IF EXISTS;

TRUNCATE TABLE smguserhistory;
DROP TABLE smguserhistory;

TRUNCATE TABLE smgposition;
DROP TABLE smgposition;

TRUNCATE TABLE smgportfolio;
DROP TABLE smgportfolio

TRUNCATE TABLE smguser;
DROP TABLE smguser;

CREATE TABLE cryptotopofbook
(
    sequenceno bigint,
    symbol varchar(30) NOT NULL,
    bestbid decimal(18,8),
    bestoffer decimal(18,8),
    timestamp datetime,
    PRIMARY KEY (symbol)
);

CREATE TABLE smgfill
(
	ordersystem varchar(50),
	orderId varchar(40) not NULL,
	fillId varchar(40) not NULL,
    qty decimal(15,6),
    price decimal(18,8),
	created datetime,
	refId varchar(40),
    PRIMARY KEY (orderId,fillId)
);

CREATE TABLE smgorder
(
	ordersystem varchar(50),
	orderId varchar(40) not NULL,
    parentId varchar(40),
    symbol varchar(20),
    side varchar(10),
    qty decimal(15,6),
    doneqty decimal(15,6),
    openqty decimal(15,6),
    price decimal(18,8),
	limitprice decimal(18,8),
	created datetime,
    lastupdate datetime,
    ordtype int,
    tif varchar(10),
    state int,
    extorderid varchar(40),
    extsystem varchar(50),
    PRIMARY KEY (orderid)
);

CREATE TABLE smguser
(
    userid int AUTO_INCREMENT PRIMARY KEY,
    username varchar(30),
    password varchar(30),
    fullname varchar(50),
    email varchar(100)
);

CREATE TABLE smguserhistory
(
    userid int NOT NULL,
    lastupdate datetime,
    status varchar(20),
    FOREIGN KEY (userid),
    REFERENCES smguser(userid)
);

CREATE TABLE smgportfolio
(
    userid int NOT NULL,
    amount decimal(18,6),
    created datetime,
    lastupdate datetime,
    FOREIGN KEY (userid)
    REFERENCES smguser (userid)
);

CREATE TABLE smgposition
(
    userid int NOT NULL,
    symbol varchar(30),
    amount decimal(18,6),
    created datetime,
    lastupdate datetime,
    FOREIGN KEY (userid)
    REFERENCES smguser (userid)
);