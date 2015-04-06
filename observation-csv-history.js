
/**
 * OpenIoD module for connecting ILM sensor system 
 *
 * @param  {String} ##todo 
 * @return {String}
 */
 
 "use strict";

 
var fs 		= require('fs');
var request = require('request');
var sys 	= require('sys');
//var cassandra = require('../../openiod-cassandra');
var MongoClient = require('mongodb').MongoClient
    , format = require('util').format;
	 
var localModelFolders 	= [];
var models 				= {};

var csvHistoryUrl = 'http://82.201.127.232:8080/csv/';
var airboxCsvFileName = '25_cal.csv';
var tmpFolder;


module.exports = {

	
	reqCsvHistory: function (featureOfInterest, param, callback) {

		airboxCsvFileName 		= param.query.file;

		var aireasLocalPathRoot = __dirname + '/../../';
//		fileFolder 			= '';
//		tmpFolder 			= aireasLocalPathRoot + fileFolder + "/" + 'tmp/';
		tmpFolder 			= aireasLocalPathRoot;

		// create subfolders
//		try {fs.mkdirSync(tmpFolder );} catch (e) {};//console.log('ERROR: no tmp folder found, batch run aborted.'); return } ;

		// 10-minuten reeksen met actuele AiREAS luchtmetingen. Verversing elke 10 minuten.
		
		if (param.query.file != null ) {
			var observationFile = fs.readFileSync(airboxCsvFileName);
			this.createCql(observationFile);
		} else {
			this.streamCsvHistoryFile (csvHistoryUrl + airboxCsvFileName, airboxCsvFileName,	false, 'aireascsvdata', callback);
		}

		console.log('All retrieve actions are activated.');

	},

	convertGPS2LatLng: function(gpsValue) {
		var degrees = Math.floor(gpsValue /100);
		var minutes = gpsValue - (degrees*100);
		var result  = degrees + (minutes /60);
		return result;
	},

	createCql: function(inpFile) {
		
			//cassandra.init();
			
			var _dataRecord; 
			var _dataRecordPrevious={};
			
			var insertQuery;
			var inpFileString = inpFile.toString();
			var _status='initial';
			
			var tmpArray = inpFileString.split('\n');
			var outFile = '';
			
			var counter =0;

			var historyArray = [];


			MongoClient.connect('mongodb://192.168.0.92:27017/openiod', function(err, db) {
 		  	 	if(err) throw err;

				var collection = db.collection('observation');
				
				console.log('Collection: ' + collection);
				/*
    			collection.insert({a:2}, function(err, docs) {

      				collection.count(function(err, count) {
        				console.log(format("count = %s", count));
      				});

					// Locate all the entries using find
      				collection.find().toArray(function(err, results) {
        				console.dir(results);
        				// Let's close the db
        				db.close();
      				});
				});
	
	*/



			
			
			for(var i=1;i<tmpArray.length-1;i++) {  // start i=1 !!

				//inpRecordArray 		= tmpArray[i].split(':(');

				_dataRecord			= {};
				//_dataRecord.airBox	= inpRecordArray[0];

				var inpMetingenArray 	= tmpArray[i].split(',');
		
				var _waardeDataRecord 	= [];
				for(var j=0;j<inpMetingenArray.length;j++) {
					_waardeDataRecord[j] = inpMetingenArray[j];// parseFloat(inpMetingenArray[j]);
				}

				_dataRecord.phenomenonDateChar 	= _waardeDataRecord[9].substr(0,19);
				_dataRecord.phenomenonDate = new Date(_dataRecord.phenomenonDateChar);
				_dataRecord.measureDate 	= "";  // not yet as key/value in measure data
				_dataRecord.gpsLat 	= _waardeDataRecord[0];
				_dataRecord.gpsLng 	= _waardeDataRecord[1];
				_dataRecord.lat 	= this.convertGPS2LatLng(_waardeDataRecord[0]);
				_dataRecord.lng 	= this.convertGPS2LatLng(_waardeDataRecord[1]);
				_dataRecord.UFP 	= _waardeDataRecord[2];
				_dataRecord.OZON 	= _waardeDataRecord[3];
				_dataRecord.PM10 	= _waardeDataRecord[4];
				_dataRecord.PM1 	= _waardeDataRecord[5];
				_dataRecord.PM25 	= _waardeDataRecord[6];
				_dataRecord.HUM 	= _waardeDataRecord[7];
				_dataRecord.CELC 	= _waardeDataRecord[8];

				_dataRecord.gpsLatFloat = parseFloat(_waardeDataRecord[0]);
				_dataRecord.gpsLngFloat	= parseFloat(_waardeDataRecord[1]);
				_dataRecord.UFPFloat 	= parseFloat(_waardeDataRecord[2]);
				_dataRecord.OZONFloat 	= parseFloat(_waardeDataRecord[3]);
				_dataRecord.PM10Float 	= parseFloat(_waardeDataRecord[4]);
				_dataRecord.PM1Float 	= parseFloat(_waardeDataRecord[5]);
				_dataRecord.PM25Float 	= parseFloat(_waardeDataRecord[6]);
				_dataRecord.HUMFloat 	= parseFloat(_waardeDataRecord[7]);
				_dataRecord.CELCFloat 	= parseFloat(_waardeDataRecord[8]);

				//dataRecords.push(_dataRecord);	
				
				_status = 'active';
				if (_dataRecord.gpsLatFloat == 0 || _dataRecord.lat - _dataRecordPrevious.lat > 0.1 ) {
					_status = 'maintenance';				
				} else {
					if ( _dataRecord.UFPFloat 	== _dataRecordPrevious.UFPFloat &&
						_dataRecord.OZONFloat 	== _dataRecordPrevious.OZONFloat &&
						_dataRecord.PM10Float 	== _dataRecordPrevious.PM10Float &&
						_dataRecord.PM1Float 	== _dataRecordPrevious.PM1Float &&
						_dataRecord.PM25Float 	== _dataRecordPrevious.PM25Float //&&
					//	_dataRecord.HUMFloat 	== _dataRecordPrevious.HUMFloat &&
					//	_dataRecord.CELCFloat 	== _dataRecordPrevious.CELCFloat					
					) {
						_status = 'blocked';
					}
				}

				//		if (i>50) {
				//			console.log(i);
				//			break;
				//		}
				
	
				historyArray.push(_dataRecord);
				
			}	
			
			historyArray.sort(function(a,b){return a.phenomenonDate.getTime() - b.phenomenonDate.getTime() });
				

			counter = historyArray.length;

			for(var j=0;j<historyArray.length;j++) {
						
				_dataRecord = historyArray[j];
				
				
				
//				insertQuery = "INSERT INTO observation ( systemUuid, systemId, foiUuid, foiId, modelId, phenomenonTime, phenomenonTimeChar, epsg, lat, lng, status, sweFieldNames, sweFieldValues, sweFieldUoms, mutationTimeUuid, mutationBy) VALUES( 2a1c1d09-c044-447c-9346-1b40692c59e6, 'ILM', d46a9592-3f38-436c-9e94-4e82d0f798b3, '25.cal', 'P1-25-10-UOHT', minTimeuuid('"+ _dataRecord.phenomenonTime + "'), '" + _dataRecord.phenomenonTime + "', " + "'4326', " + _dataRecord.lat + ", " + _dataRecord.lng + ", '" + _status + "' " + ", ['PM1', 'PM25', 'PM10', 'UFP', 'OZON', 'HUM', 'CELC']" + ", [" +_dataRecord.PM1 + ", " + _dataRecord.PM25 + ", " + _dataRecord.PM10 + ", " + _dataRecord.UFP + ", " + _dataRecord.OZON + ", " + _dataRecord.HUM + ", " + _dataRecord.CELC + "], ['ugm3', 'ugm3', 'ugm3', 'countm3', 'ugm3', 'per', 'cel'], now(), 'system' );\r\n";
				
				//cassandra.executeCql(insertQuery, {}, function(err, result) {
				//	//console.log('Callback cassandra.executeCql Insert observation');
				//	//console.log('Query: ' + insertQuery);
				//});
				
				var collectionObject = {};
				collectionObject._id= {};
				collectionObject._id.systemUuid 	= '2a1c1d09-c044-447c-9346-1b40692c59e6';
				collectionObject._id.foiUuid 		= 'd46a9592-3f38-436c-9e94-4e82d0f798b3';
				collectionObject._id.phenomenonDate = _dataRecord.phenomenonDate;
				collectionObject.systemId 			= 'ILM';
				collectionObject.foiId 				= '25.cal';
				collectionObject.modelId 			= 'P1-25-10-UOHT';
				collectionObject.phenomenonDateChar = _dataRecord.phenomenonDateChar;
				collectionObject.epsg 				= '4326';
				collectionObject.lat 				= _dataRecord.lat;
				collectionObject.lng 				= _dataRecord.lng;
				collectionObject.status 			= _status;
				collectionObject.UFPFloat 			= _dataRecord.UFPFloat;
				collectionObject.OZONFloat 			= _dataRecord.OZONFloat;
				collectionObject.PM10Float 			= _dataRecord.PM10Float;
				collectionObject.PM25Float 			= _dataRecord.PM25Float;
				collectionObject.PM1Float 			= _dataRecord.PM1Float;
				collectionObject.HUMFloat 			= _dataRecord.HUMFloat;
				collectionObject.CELCFloat 			= _dataRecord.CELCFloat;
				
				
/*
				collection.insert({a:2}, function(err, docs) {
					console.log('mongodb test insert a:2 err: ' + err);
					
					
					
					
      			//collection.count(function(err, count) {
        		//	console.log(format("count = %s", count));
      			//});

				// Locate all the entries using find
      			//collection.find().toArray(function(err, results) {
        		//	console.dir(results);
        	//		// Let's close the db
        //			db.close();
      	//		});
			});
*/


//{"systemUuid":"2a1c1d09-c044-447c-9346-1b40692c59e6","systemId":"ILM","foiUuid":"d46a9592-3f38-436c-9e94-4e82d0f798b3","foiId":"25.cal","modelId":"P1-25-10-UOHT","phenomenonTimeChar":"2000-01-01 00:00:00","epsg":"4326","lat":0,"lng":0,"status":"maintenance"}
				
				//var collectionObjectJson = JSON.stringify(collectionObject);
				
				//console.log('Collection insert: ' + collectionObject );
				
				
				collection.save(collectionObject, function(err, docs) {
				
					if (err) {
						console.log('mongodb insert err: ' + counter + ' ' + err);
					}
					
					counter--;
					if (counter <= 0) {
						console.log('Counter is ' + counter + ' closing the database.');
						db.close();
						callback();
					}

					if (counter <= 10) {
						console.log('Counter is ' + counter );
					}
					
					
      			//	collection.count(function(err, count) {
        		//		console.log(format("count = %s", count));
      			});

					// Locate all the entries using find
     // 				collection.find().toArray(function(err, results) {
     //   				console.dir(results);
     //   				// Let's close the db
     //   				db.close();
     // 				});
				//});
				
				
		//		if (i>17000) {
		//			console.log(i);
		//			break;
		//		}
					
				if (j == Math.floor(j/10000)*10000 ) console.log(j);
				
				_dataRecordPrevious = _dataRecord;
				
				
				/*
				
				
				db.observation.aggregate([ {$group: {
					_id: { foiId: "$_id.foiId"
						, year: { $year:"$_id.phenomenonDate" }
					    , month: { $month:"$_id.phenomenonDate" }
						, dayOfMonth: { $dayOfMonth:"$_id.phenomenonDate" }
						, status: "$status"
						  }
					, count: { $sum: 1}
					, avgUFP: { $avg: "$UFPFloat"}
					, avgPM1: { $avg: "$PM1Float"}
					, avgPM25: { $avg: "$PM25Float"}
					, avgPM10: { $avg: "$PM10Float"}
				}},
				{ $sort : { "_id.year" : -1, "_id.month" : -1, "_id.dayOfMonth" : -1, "_id.status": 1 } }
				])

				db.observation.aggregate([ {$group: {
					_id: { foiId: "$foiId", day: { $dayOfMonth:"$phenomenonDateTime" }, month: { $month:"$phenomenonDateTime" }, year: { $year:"$phenomenonDateTime" } },
					count: { $sum: 1}
				}}
				])				

				db.observation.aggregate([ {$group: {
					_id: { foiId: "$foiId", year: { $year:"$phenomenonDateTime" }, month: { $month:"$phenomenonDateTime" }, status: "$status" },
					count: { $sum: 1 }
				}},
				{ $sort : { "_id.status" : -1 } }
				])				
				
				db.observation.remove({ });
				
				
				*/

			}

				//callback(cqlFile, {}, callback2);
 				
								
			
			});

			console.log(' Total length: ' + tmpArray.length); 
			return outFile;
	},
			
	streamCsvHistoryFile: function (url, fileName, unzip, desc, callback ) {
	
		var _wfsResult=null;
		console.log("Request start: " + desc + " (" + url + ")");

		var outFile;

		function StreamBuffer(req) {
  			var self = this

  			var buffer = []
  			var ended  = false
  			var ondata = null
  			var onend  = null

  			self.ondata = function(f) {
    			console.log("self.ondata")
    			for(var i = 0; i < buffer.length; i++ ) {
      				f(buffer[i])
      				console.log(i);
    			}
    			console.log(f);
    			ondata = f
  			}

  			self.onend = function(f) {
    			onend = f
    			if( ended ) {
      				onend()
    			}
  			}

  			req.on('data', function(chunk) {
    			// console.log("req.on data: ");
    			if (_wfsResult) {
      				_wfsResult += chunk;
    			} else {
      				_wfsResult = chunk;
    			}

    			if( ondata ) {
      				ondata(chunk)
    			} else {
      				buffer.push(chunk)
    			}
  			})

  			req.on('end', function() {
    			//console.log("req.on end")
    			ended = true;

	    		if( onend ) {
   		   			onend()
    			}
  			})        
 
  			req.streambuffer = self
		}

		function writeFile(path, fileName, content) {
  			fs.writeFile(path + fileName, content, function(err) {
    			if(err) {
      				console.log(err);
    			} else {
      				console.log("The file is saved! " + tmpFolder + fileName + ' (unzip:' + unzip + ')');
					if (unzip) {
						var exec = require('child_process').exec;
						var puts = function(error, stdout, stderr) { sys.puts(stdout) }
						exec(" cd " + tmpFolder + " ;  unzip -o " + tmpFolder + fileName + " ", puts);
					}
    			}
  			});	 
		}
	
		
		

  		new StreamBuffer(request.get( { url: url }, function(error, response) {
			console.log("Request completed: " + desc + " " );
			var currDate = new Date();
			var iso8601 = currDate.toISOString();
			

	
				var cqlFile = createCql(_wfsResult);			
				console.log(' Aantal records: ' + cqlFile.length);

			//	writeFile(tmpFolder, fileName, iso8601 + ' ' + cqlFile);
			//	writeFile(tmpFolder, fileName, cqlFile);


			
			
			})
  		);

	} // end of reqFile	
	



};




