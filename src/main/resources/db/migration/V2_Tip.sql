alter table Payment add column receivedTip decimal(19,2);
alter table Payment add column spentTip decimal(19,2);
alter table Payment add column totalToSend decimal(19,2);
alter table Payment add column expiresOn DATETIME;
alter table Payment add column paidOn DATETIME;

CREATE TABLE ExpiredECKey (
  id varchar(255) NOT NULL,
  privateKey longblob,
  publicKey longblob,
  createdOn DATETIME NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;