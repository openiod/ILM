
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


module.exports = {

	
	getMongoData: function (featureOfInterest, param, callback) {
		
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

		console.log('All retrieve actions are activated. getMongoData observation-aggregation: ' + featureOfInterest );
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
					
					collectionMerge.remove({"_id.foiId":param.query.featureofinterest}, function(err, results) {
						console.log('mongodb removed old values %s %s', err, results);
					
						collectionTmp.find({}).toArray(function(err, doc) {
							if(err) {
								console.log('mongodb find for tmp collection error: ' + err);
								db.close();
							} else {
								console.log('Insert new records: ' );
						
								for (var i=0;i<doc.length;i++) {
									collectionMerge.save(doc[i], function(err, result) {
										console.log('Record saved');
									});
								}
								console.log('Inserted %s records', doc.length );						
					
								console.log('Drop temporary collection: ' + param.collectionTmp);
								collectionTmp.drop();
	
								console.log('Closing the database.');
								console.log(' Data: ' + doc.length);
								db.close();
								_callback(doc);
							}	
						 });
					 });
				} 

      		});
			};
		});
	}	
	
	
	


};




