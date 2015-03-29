# OpenIoD Connector ILM

THIS IS WORK IN PROGRESS. DON'T WORRY, UPDATES WILL CONTINUE THE WORK ;-)

#Metadata

## Feature of Interest
The feature of interest is in the case of the ILM-system, the box or Airbox containing the sensors.

Every feature of interest has an unique ID (nodeUuid). For the combination of nodeUuid and start-/endtime period a record exists in the database. More then one time period (record) can exist for an nodeUuid but time periods may not overlap.

Attributes:

Name|example
-------|--------
systemId|'ILM'
nodeUuid|'de305d54-75b4-431b-adb2-eb6b9e546013'
nodeId|'23.cal'
nodeName|'23.cal'
modelId|'P1-25-10-U-T-H'
startTime|'2015-03-29T01:01:01+0000'  (timezone diff from UTC)
endTime|'4000-12-31T00:00:00+0000'  (timezone diff from UTC)
status| _see status description table below_
EPSG|4326
lat|51.43657
lng|5.4863
locationName|'Twickel'
locationPrefix|''
locationCode|'5655 JJ'
locationCity|'Eindhoven'
logEmail|['support@example.com','log@example.com']

##Status descriptions
Status|Description
------|-----------
inactive|not in use
ready|ready state, no data yet
active|Active state, produced data
maintenance|in maintenance, not producing data or testdata only. Historical data available
blocked|In case of defect. No data available

See [this file](./Cassandra/create_table_featureofinterest.cql) for the create table statement for a Cassandra NoSQL database.
	

	

# Copyright and disclaimer

Copyright (C) 2015, Scapeler

Permission to use, copy, modify, and/or distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
