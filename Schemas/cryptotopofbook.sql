TRUNCATE TABLE cryptotopofbook;
DROP TABLE cryptotopofbook;

CREATE TABLE cryptotopofbook
(
    sequenceno bigint,
    symbol varchar(30) NOT NULL,
    bestbid decimal(18,8),
    bestoffer decimal(18,8),
    timestamp datetime,
    PRIMARY KEY (symbol)
);
