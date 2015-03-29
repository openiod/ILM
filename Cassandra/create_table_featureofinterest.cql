// work in progress !!

CREATE TABLE "featureofinterest" (
  systemId varchar,
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
  PRIMARY KEY ((systemId, nodeId), startTime)
) WITH CLUSTERING ORDER BY ( startTime DESC);
