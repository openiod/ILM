// work in progress !!

CREATE TABLE "featureofinterest" (
systemId varchar,
nodeId uuid,
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
locationPostCode varchar,
locationCity varchar,
PRIMARY KEY ((systemId, nodeId), observationTime)
) WITH CLUSTERING ORDER BY ( observationTime DESC);

