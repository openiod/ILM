// work in progress !!

//Cassandra database for OpenIoD

// drop table featureofinterest;

CREATE TABLE "featureofinterest" (
  systemUuid uuid,
  systemId varchar,
  systemName varchar,
  nodeUuid uuid,
  nodeId varchar,
  nodeName varchar,
  modelId varchar,
  startTime timestamp,
  endTime timestamp,
  epsg varchar,
  lat float,
  lng float,
  status varchar,
  locationName varchar,
  locationPrefix varchar,
  locationCode varchar,
  locationCity varchar,
  mutationDate timestamp,
  mutationBy varchar,
  PRIMARY KEY ((systemUuid, nodeUuid), startTime)
) WITH CLUSTERING ORDER BY ( startTime DESC);
