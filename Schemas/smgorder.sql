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
    price decimal(15,6),
	limitprice decimal(15,6),
	created datetime,
    lastupdate datetime,
    ordtype int,
    tif varchar(10),
    state int,
    extorderid varchar(40),
    extsystem varchar(50),
    PRIMARY KEY (orderid)
);