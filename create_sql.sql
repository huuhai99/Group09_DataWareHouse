use datawarehouse;

CREATE TABLE config(
  id bigint NOT NULL PRIMARY KEY auto_increment,
  tableName varchar (100),
  source varchar (100),
  destination varchar(100),
  pathFile varchar (100),
  userName varchar(100),
  password varchar (100)

);
