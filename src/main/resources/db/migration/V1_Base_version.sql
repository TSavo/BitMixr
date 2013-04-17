CREATE TABLE Payment (
  id varchar(255) NOT NULL,
  privateKey longblob,
  publicKey longblob,
  sourceAddress varchar(255) DEFAULT NULL,
  destinationAddress varchar(255) DEFAULT NULL,
  recievedAmount decimal(19,2) DEFAULT NULL,
  sentAmount decimal(19,2) DEFAULT NULL,
  spentAmount decimal(19,2) DEFAULT NULL,
  createdOn DATETIME NOT NULL,
  updatedOn DATETIME,
  visible bit(1) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE SeenTransaction (
  id varchar(255) NOT NULL,
  transactionHash varchar(255) NOT NULL,
  paymentId varchar(255) NOT NULL,
  seenOn DATETIME NOT NULL,
  amount decimal(19,2),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;