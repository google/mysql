CREATE TABLE CommittedStatsVersions (TableName varchar(128) NOT NULL default '', LastVersion int(11) NOT NULL default '0', CumulativeVersionHash bigint(20) NOT NULL default '0', PRIMARY KEY  (TableName)) ENGINE=InnoDB;

CREATE TABLE LocalStatsServers (Host VARCHAR(128) NOT NULL DEFAULT '', Port INT(11) NOT NULL DEFAULT 0, TableName VARCHAR(128) NOT NULL DEFAULT '', Tier INT(11) NOT NULL DEFAULT 0, PRIMARY KEY (TableName, Host, Port)) ENGINE=InnoDB DEFAULT CHARSET=latin1;
