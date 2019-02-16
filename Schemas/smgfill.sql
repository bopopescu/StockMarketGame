CREATE TABLE smgfill
(
	ordersystem varchar(50),
	orderId varchar(40) not NULL,
	fillId varchar(40) not NULL,
    qty decimal(15,6),
    price decimal(15,6),
	created datetime,
	refId varchar(40),
    PRIMARY KEY (orderId,fillId)
);