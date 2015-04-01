
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
var MongoClient = require('../openiod/node_modules/mongodb').MongoClient
    , format = require('util').format;
	 
var localModelFolders 	= [];
var models 				= {};

var csvHistoryUrl = 'http://82.201.127.232:8080/csv/';
var airboxCsvFileName = '25_cal.csv';
var tmpFolder;


module.exports = {


	loadAllModels: function(folder) {
	
		var context = this;
		var modelLocalPath = __dirname+'/model/';
		var localModelIndex = -1;
		localModelFolders=[]; // reset localModels array
		fs.readdir(modelLocalPath, function (err, files) {
			if (err) { console.log("Local model folder not found: " + modelLocalPath);
			} else {
				localModelFolders 	= files;
  				console.log("Local models: " + localModelFolders.toString());
				
				for (var i=0;i<localModelFolders.length;i++) {
					if (localModelFolders[i] == 'README.md' ) {
						continue;
					}
					context.loadModel(localModelFolders[i]);
				}
			}
		});
	},

	loadModel: function(modelFolderName) {
	
		var modelFolderLocalPath = __dirname+'/model/'+modelFolderName;
		
		var sweDataRecordJson = fs.readFileSync(modelFolderLocalPath+'/datarecord.json');
		models[modelFolderName] = {};
		var model = models[modelFolderName];
		console.log('model: ' +  modelFolderName);
		//console.log('  data: ' + sweDataRecordJson);
		var _sweDataRecord = JSON.parse(sweDataRecordJson);
		model.sweDataRecord = _sweDataRecord.sweDataRecord;
	},

	getModel: function(modelName) {	
		return models[modelName];		
	},
	
	getModels: function() {	
		return models;		
	},
	
	reqCsvHistory: function (options, callback, callback2) {

		airboxCsvFileName 		= '25_cal.csv';

		var aireasLocalPathRoot = __dirname + '/../../';
//		fileFolder 			= '';
//		tmpFolder 			= aireasLocalPathRoot + fileFolder + "/" + 'tmp/';
		tmpFolder 			= aireasLocalPathRoot;

		// create subfolders
//		try {fs.mkdirSync(tmpFolder );} catch (e) {};//console.log('ERROR: no tmp folder found, batch run aborted.'); return } ;

		// 10-minuten reeksen met actuele AiREAS luchtmetingen. Verversing elke 10 minuten.
	
		this.streamCsvHistoryFile (csvHistoryUrl + airboxCsvFileName, airboxCsvFileName,	false, 'aireascsvdata', callback, callback2);

		console.log('All retrieve actions are activated.');

	},
	
	streamCsvHistoryFile: function (url, fileName, unzip, desc, callback, callback2 ) {
	
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
	
		function convertGPS2LatLng(gpsValue){
			var degrees = Math.floor(gpsValue /100);
			var minutes = gpsValue - (degrees*100);
			var result  = degrees + (minutes /60);
			return result;
		}
		
		function createCql(inpFile, collection) {
		
			//cassandra.init();
			
			var _dataRecord; 
			var _dataRecordPrevious={};
			
			var insertQuery;
			var inpFileString = inpFile.toString();
			var _status='initial';
			
			var tmpArray = inpFileString.split('\n');
			var outFile = '';
		
			for(var i=1;i<tmpArray.length-1;i++) {  // start i=1 !!

				//inpRecordArray 		= tmpArray[i].split(':(');

				_dataRecord			= {};
				//_dataRecord.airBox	= inpRecordArray[0];

				var inpMetingenArray 	= tmpArray[i].split(',');
		
				var _waardeDataRecord 	= [];
				for(var j=0;j<inpMetingenArray.length;j++) {
					_waardeDataRecord[j] = inpMetingenArray[j];// parseFloat(inpMetingenArray[j]);
				}

				_dataRecord.phenomenonTime 	= _waardeDataRecord[9].substr(0,19);
				_dataRecord.measureDate 	= "";  // not yet as key/value in measure data
				_dataRecord.gpsLat 	= _waardeDataRecord[0];
				_dataRecord.gpsLng 	= _waardeDataRecord[1];
				_dataRecord.lat 	= convertGPS2LatLng(_waardeDataRecord[0]);
				_dataRecord.lng 	= convertGPS2LatLng(_waardeDataRecord[1]);
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
						_dataRecord.PM25Float 	== _dataRecordPrevious.PM25Float &&
						_dataRecord.HUMFloat 	== _dataRecordPrevious.HUMFloat &&
						_dataRecord.CELCFloat 	== _dataRecordPrevious.CELCFloat					
					) {
						_status = 'blocked';
					}
				}
				
				
//				insertQuery = "INSERT INTO observation ( systemUuid, systemId, foiUuid, foiId, modelId, phenomenonTime, phenomenonTimeChar, epsg, lat, lng, status, sweFieldNames, sweFieldValues, sweFieldUoms, mutationTimeUuid, mutationBy) VALUES( 2a1c1d09-c044-447c-9346-1b40692c59e6, 'ILM', d46a9592-3f38-436c-9e94-4e82d0f798b3, '25.cal', 'P1-25-10-UOHT', minTimeuuid('"+ _dataRecord.phenomenonTime + "'), '" + _dataRecord.phenomenonTime + "', " + "'4326', " + _dataRecord.lat + ", " + _dataRecord.lng + ", '" + _status + "' " + ", ['PM1', 'PM25', 'PM10', 'UFP', 'OZON', 'HUM', 'CELC']" + ", [" +_dataRecord.PM1 + ", " + _dataRecord.PM25 + ", " + _dataRecord.PM10 + ", " + _dataRecord.UFP + ", " + _dataRecord.OZON + ", " + _dataRecord.HUM + ", " + _dataRecord.CELC + "], ['ugm3', 'ugm3', 'ugm3', 'countm3', 'ugm3', 'per', 'cel'], now(), 'system' );\r\n";
				
				//cassandra.executeCql(insertQuery, {}, function(err, result) {
				//	//console.log('Callback cassandra.executeCql Insert observation');
				//	//console.log('Query: ' + insertQuery);
				//});
				
				collectionObject = {};
				collectionObject.systemUuid = '2a1c1d09-c044-447c-9346-1b40692c59e6';
				collectionObject.systemId = 'ILM';
				collectionObject.foiUuid = 'd46a9592-3f38-436c-9e94-4e82d0f798b3';
				collectionObject.foiId = '25.cal';
				collectionObject.modelId = 'P1-25-10-UOHT';
				collectionObject.phenomenonTimeChar = _dataRecord.phenomenonTime;
				
				collectionObjectJson = JSON.stringify(collectionObject);
				
				collection.insert(collectionObjectJson, function(err, docs) {

     // 				collection.count(function(err, count) {
     //   				console.log(format("count = %s", count));
     // 				});

					// Locate all the entries using find
     // 				collection.find().toArray(function(err, results) {
     //   				console.dir(results);
     //   				// Let's close the db
     //   				db.close();
     // 				});
				});
				
				
			//	if (i>2500) {
			//		console.log(i);
			//		break;
			//	}	
				if (i == Math.floor(i/10000)*10000 ) console.log(i);
				
				if ( i == (tmpArray.length-3) ) console.log('length-3: ' + _waardeDataRecord.length);
				if ( i == (tmpArray.length-2) ) console.log('length-2: ' + _waardeDataRecord.length);
				if ( i == (tmpArray.length-1) ) console.log('length-1: ' + _waardeDataRecord.length);
				
				_dataRecordPrevious = _dataRecord;

			}
			console.log(' Total length: ' + tmpArray.length); 
			return outFile;
		}
		

  		new StreamBuffer(request.get( { url: url }, function(error, response) {
			console.log("Request completed: " + desc + " " );
			var currDate = new Date();
			var iso8601 = currDate.toISOString();
			

			MongoClient.connect('mongodb://192.168.0.92:27017/openiod', function(err, db) {
 		  	 	if(err) throw err;

				var collection = db.collection('observation');
				
				
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
	
				var cqlFile = createCql(_wfsResult, collection);			
				console.log(' Aantal records: ' + cqlFile.length);

			//	writeFile(tmpFolder, fileName, iso8601 + ' ' + cqlFile);
			//	writeFile(tmpFolder, fileName, cqlFile);
			
				callback(cqlFile, {}, callback2);
 
			
			});


			
			
			})
  		);

	} // end of reqFile	
	



};




