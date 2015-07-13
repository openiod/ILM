
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

var tmpFolder;


var pg = require('pg');

var sqlConnString;



function executeSql (query, callback) {
	console.log('sql start: ');
	var client = new pg.Client(sqlConnString);
	client.connect(function(err) {
  		if(err) {
    		console.error('could not connect to postgres', err);
			callback(result, err);
			return;
  		}
  		client.query(query, function(err, result) {
    		if(err) {
      			console.error('error running query', err);
				callback(result, err);
				return;
    		}
    		//console.log('sql result: ' + result);
			callback(result.rows, err);
    		client.end();
  		});
	});
};


module.exports = {

	initDbConnection: function (options) {
		if (options.source != 'mongodb') {
			// PostgreSql
			//console.log(options);
			sqlConnString = options.param.systemParameter.databaseType + '://' + 
				options.param.systemParameter.databaseAccount + ':' + 
				options.param.systemParameter.databasePassword + '@' + 
				options.param.systemParameter.databaseServer + '/' +
				options.param.systemCode + '_' + options.param.systemParameter.databaseName;
		}
	},
	
	getData: function (featureOfInterest, param, callback) {
	
		if (param.query.source != 'mongodb') {
		
			if (sqlConnString == null) {
				this.initDbConnection({source:'postgresql', param: param });
			}
			this.getAireasHistInfo(featureOfInterest, param, callback);		
			return;
		}
		
//		if (param.query.file != null ) {
//			var observationFile = fs.readFileSync(airboxCsvFileName);
//			console.log('Observation from file: ' + observationFile.length);
//			this.createCql(observationFile, callback);
//		} else {
//			this.streamCsvHistoryFile (csvHistoryUrl + airboxCsvFileName, airboxCsvFileName,	false, 'aireascsvdata', callback);
//		}

		console.log('All retrieve actions are activated. getMongoData observation-aggregation: ' + featureOfInterest );
		console.log(' Aggregation: ' + param.query );
//		MongoClient.connect('mongodb://192.168.0.92:27017/openiod', function(err, db) {
		MongoClient.connect('mongodb://149.210.201.210:27017/openiod', function(err, db) {
	  	 	if(err) throw err;

			var collection = db.collection(param.collection );
			
			console.log('Collection: ' + param.collection);
			console.log('   aggregation: ' + JSON.stringify(param.aggregation) );

			collection.aggregate(param.aggregation).toArray(function(err, results) {
				
				if (err) {
					console.log('mongodb find err: ' + err);
				}
				
				//var results = {};
				//results.
					
				console.log('Closing the database.');
				console.log(' Data: ' + results.length);
				db.close();
				callback(results);

      		});
		});
	},
	
	// Merge db.collection1.find().forEach(function(doc){db.collection2.save(doc)});
	merge: function (featureOfInterest, param, callback) {
	
		var _callback = callback;
		
//		if (param.query.file != null ) {
//			var observationFile = fs.readFileSync(airboxCsvFileName);
//			console.log('Observation from file: ' + observationFile.length);
//			this.createCql(observationFile, callback);
//		} else {
//			this.streamCsvHistoryFile (csvHistoryUrl + airboxCsvFileName, airboxCsvFileName,	false, 'aireascsvdata', callback);
//		}

		console.log('All retrieve actions are activated. getMongoData observation-aggregation: ' + param.query.featureofinterest );
		console.log(' Aggregation: ' + param.query );

		MongoClient.connect('mongodb://149.210.201.210:27017/openiod', function(err, db) {
	  	 	if (err) {
				console.log('mongodb connection error: ' + err);
				db.close();
			} else {
			
			var collection = db.collection(param.collection );
			
			console.log('Collection: ' + param.collection);
			console.log('   aggregation: ' + JSON.stringify(param.aggregation) );

			collection.aggregate(param.aggregation, function(err, results) {
				if (err) {
					console.log('mongodb aggregate error: ' + err);
					db.close();
				} else {
				
					var collectionTmp 	= db.collection(param.collectionTmp );   // contains aggregated results				
					var collectionMerge = db.collection(param.collectionMerge ); // this is the destination collection
				
					console.log('End of aggregate function into collection: ' + param.collectionTmp);
					console.log('Start removing old values in collection: %s %s', param.collectionTmp, param.query.featureofinterest);
					
					collectionMerge.remove({"_id.foiId":param.query.featureofinterest}, function(err, results) {
						console.log('mongodb removed old values %s %s', err, results);
					
						collectionTmp.find({}).toArray(function(err, doc) {
							if(err) {
								console.log('mongodb find for tmp collection error: ' + err);
								db.close();
							} else {
								console.log('Insert new records in bulk for %s', param.query.featureofinterest );
								
								var batch = collectionMerge.initializeUnorderedBulkOp({useLegacyOps: true});
						
								for (var i=0;i<doc.length;i++) {
									batch.insert(doc[i]);
//									collectionMerge.save(doc[i], function(err, result) {
										//console.log('Record inserted into batch');
//									});
								}
								batch.execute(function(err, result) {
									console.log('Batch insert executed');
									console.log('Inserted %s records', doc.length );						
					
									console.log('Drop temporary collection: ' + param.collectionTmp);
									collectionTmp.drop();
	
									console.log('Closing the database.');
									console.log(' Data: ' + doc.length);
									db.close();
									_callback(doc);
								});								
							}	
						 });
					 });
				} 

      		});
			};
			
			
			
			
		});
	},
	

	getAireasHistInfo: function (featureofinterest, param, callback) {
		var _airbox = "";
		var currentDate = new Date();
		
		if (param.query.avgType == undefined || param.query.avgType == '') {
			param.query.avgType = 'PM10';
		}
		
		if (param.query.histYear == undefined || param.query.histYear == '') {
			param.query.histYear = currentDate.getFullYear().toString();
		}

		if (param.query.histMonth == undefined || param.query.histMonth == '') {
			param.query.histMonth = undefined;
			param.query.histDay = undefined;  // overrule parameter when no month given
		} // else {
			// month nr or 'all'
		//}

		if (param.query.histDay == undefined || param.query.histDay == '') {
			param.query.histDay = undefined;
		} //else {	
			// day nr or 'all'
		//}



		var querySelect = " select a.airbox, a.hist_year, a.hist_month, a.hist_day, a.hist_count, a.last_measuredate, \
			a.avg_type, a.avg_avg ";
//  		ST_AsGeoJSON(ST_Transform(a.geom, 4326)) geom, \

		var queryFrom = " from aireas_hist_avg a ";
		var queryWhere = " where 1=1  ";
		
			if (param.query.avgType != undefined && param.query.avgType != 'all') {
				queryWhere += " and a.avg_type = '" + param.query.avgType + "' ";
			}
			if (param.query.histYear != undefined && param.query.histYear != 'all') {
				queryWhere += " and a.hist_year = " + param.query.histYear + " ";
			}
			if (param.query.histMonth == undefined) {
				queryWhere += " and a.hist_month is null ";
				queryWhere += " and a.hist_day is null ";
			} else {
				if (param.query.histMonth == 'all') {
					queryWhere += " and a.hist_month is not null ";
				} else {
					queryWhere += " and a.hist_month = " + param.query.histMonth + " ";
				}
				if (param.query.histDay == undefined) {
					queryWhere += " and a.hist_day is null ";
				} else {
					if (param.query.histDay == 'all') {
						queryWhere += " and a.hist_day is not null ";
					} else {	
						queryWhere += " and a.hist_day = " + param.query.histDay + " ";
					}
				}
			}	

		var queryGroupBy = ""; // group by grid.gm_code, grid.gm_naam, grid.cell_geom"; //, grid.centroid_geom ";
		var queryOrderBy = " ORDER BY airbox, hist_year, hist_month, hist_day "; 
		
		console.log('Postgres sql start execute');
		var query = querySelect + queryFrom + queryWhere + queryGroupBy + queryOrderBy;
		console.log('Query: ' + query);
		executeSql(query, callback);

        return;
	},


		
	// not yet in use
	getGridGemAireasHistInfo: function (param, req_query, callback) {
		var _airbox = "";
		
		if (req_query.avgType == undefined) {
			req_query.avgType = 'SPMI';
		}

		var querySelect = " select grid.gm_code, grid.gm_naam, cellunion.hist_year, cellunion.hist_month, cellunion.hist_day, \
  			ST_AsGeoJSON(ST_Transform(cellunion.union_geom, 4326)) geom, \
			ST_AsGeoJSON(ST_Transform(ST_Centroid(cellunion.union_geom), 4326)) centroid, \
			cell.cell_x, cell.cell_y, \
			cellunion.avg_type, cellunion.avg_avg ";
//			cellunion.avg_pm1_hr, cellunion.avg_pm25_hr, cellunion.avg_pm10_hr, cellunion.avg_pm_all_hr ";
			

		retrieveddateMaxConstraintStr = "";  // 2014-11-09T09:30:01.376Z

		if ( req_query.retrieveddatemax) {  //todo
			console.log('req_query: ' + req_query );
			retrieveddateMaxConstraintStr = " and cellunion2.retrieveddate AT TIME ZONE 'UTC' <= timestamp '" + req_query.retrieveddatemax + "' ";  //'2014-11-09T06:00:01.376Z' ";

		}

		var queryFrom = " from grid_gem grid, grid_gem_cell cell, grid_gem_cell_hist_union cellunion  ";
		var queryWhere = " where grid.gm_naam = 'Eindhoven' and grid.grid_code = 'EHV20141104:1' and grid.grid_code = cell.grid_code and cell.gid = cellunion.grid_gem_cell_gid ";
			queryWhere += " and cellunion.avg_type = '" + req_query.avgType + "' ";
			queryWhere += " and cellunion.hist_year = " + req_query.hist_year + " ";
			queryWhere += " and cellunion.hist_month is null ";

		//	queryWhere += " and ST_Intersects(grid.cell_geom, a1.geom) ";
		//	queryWhere += " and a1.retrieveddate >= current_timestamp - interval '1 hour' ";
		var queryGroupBy = ""; // group by grid.gm_code, grid.gm_naam, grid.cell_geom"; //, grid.centroid_geom ";
		var queryOrderBy = ""; //" order by bu_naam ; ";

		console.log('Postgres sql start execute');
		var query = querySelect + queryFrom + queryWhere + queryGroupBy + queryOrderBy;
		console.log('Query: ' + query);
		executeSql(query, callback);

        return;
	}

	


};




