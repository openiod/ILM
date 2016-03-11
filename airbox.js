
/**
 * OpenIoD module for connecting ILM sensor system and airbox (meta) data
 *
 * @param  {String} ##todo 
 * @return {String}
 */
 
 "use strict";

 
// var fs 		= require('fs');
// var request 	= require('request');
// var sys 		= require('sys');
 
var pg = require('pg');
var QueryStream = require('pg-query-stream');
var JSONStream = require('JSONStream');
var map = require('map-stream');
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


function executeSqlStream (query, callback) {
	console.log('sql stream start: ');
	var client = new pg.Client(sqlConnString);
	var self = this;
	
	var mapCallBackTest = function(data) {
		console.log('map callback test');
		callback(data);
	}
	client.connect(function(err) {
  		if(err) {
    		console.error('could not connect to postgres', err);
			callback(result, err);
			return;
  		}
		
		var queryStream = new QueryStream(query);
  		var stream = client.query(queryStream);
  		//release the client when the stream is finished
  		stream.on('end', callback);
//  		stream.pipe(JSONStream.stringify()).pipe(process.stdout);
//  		stream.pipe(JSONStream.stringify()).pipe(process.stdout);
var teller=0;
  		stream.pipe(map(function(data, mapCallBack){
			teller+=1;
			console.log(teller + ' ' + data.airbox);
//			console.log(callback);
//			setTimeout(mapCallBack(null, data), 30000);

			var mapCallBackTest2 = function (data2) {
				console.log('test2');
				mapCallBackTest(data2);
			}
	//		console.log(setTimeout);
			setTimeout(mapCallBackTest2, 1000, data);
			mapCallBack(null, data);
			//var myFunction = function callback(null, data);
		}));
		
/*
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
*/
	});
};

/*
//pipe 1,000,000 rows to stdout without blowing up your memory usage
pg.connect(function(err, client, done) {
  if(err) throw err;
  var query = new QueryStream('SELECT * FROM generate_series(0, $1) num', [1000000])
  var stream = client.query(query)
  //release the client when the stream is finished
  stream.on('end', done)
  stream.pipe(JSONStream.stringify()).pipe(process.stdout)
})

*/

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
		if (sqlConnString == null) {
			this.initDbConnection({source:'postgresql', param: param });
		}

		if (param.action=='EcnHistoryYearAvg') {
			this.getAireasEcnHistoryYearAvgAllAirboxes(param, callback);
			return;
		}		

		if (featureOfInterest ==  'all') {
			this.getAirboxDataAllAirboxes(param, callback);
		}	
	},

	
	getAirboxDataAllAirboxes: function (param, callback) {
		var _attribute, _and;
		var _attribute 	= " airbox, airbox_type, airbox_location, airbox_location_desc, region, airbox_location_type, airbox_postcode, airbox_x, airbox_y, lat, lng ";
		var _from 		= " airbox a ";
		//var _where 		= " 1=1 ";
		//var _groupBy	= "  ";
		//var _orderBy	= _groupBy;
		var _orderBy = ' airbox ';
		
		var query = 'select ' + _attribute + ' from ' + _from + //' where ' + _where + ' group by ' + _groupBy + 
		' order by ' + _orderBy + ' ;';
		
		console.log('Postgres sql start execute: ' + query);
		executeSql(query, callback);

        return;
    },
	

	getAireasEcnHistoryYearAvgAllAirboxes: function (param, callback) {
		var _attribute, _and;

		var _attribute 	= " a.lat lat, a.lng lng, a.region region, ae.tick_date, to_number(a.airbox, '99') airbox, ae.pm1, ae.pm25, ae.pm10, ae.ufp, ae.ozone, ae.rhumext, ae.tempext, ae.no2 ";
		var _from 		= " aireas_histecn ae, airbox a ";
		var _where 		= " 1=1 and ae.airbox || '.cal' = a.airbox and extract(year from (ae.tick_date - interval '1 hour')) = 2015 and extract(month from (ae.tick_date - interval '1 hour')) = 01 ";
	//	var _groupBy	= " hist_year, a.airbox  ";
	//	var _orderBy	= _groupBy;
		
		var query = 'select ' + _attribute + ' from ' + _from + ' where ' + _where + 
		//' group by ' + _groupBy + 
		//' order by ' + _orderBy + 
		';' ;
		
		console.log('Postgres sql start execute: ' + query);
		executeSqlStream(query, callback);

        return;
    },	

	getAireasEcnHistoryYearAvgAllAirboxesBewaar: function (param, callback) {
		var _attribute, _and;

		var _attribute 	= " max(a.lat) lat, max(a.lng) lng, max(a.region) region, extract(year from (ae.tick_date - interval '1 hour')) hist_year, to_number(a.airbox, '99') airbox, round(CAST(avg(ae.pm1) AS numeric),2) pm1, round(CAST(avg(ae.pm25) AS numeric),2) pm25, round(CAST(avg(ae.pm10) AS numeric),2) pm10, round(CAST(avg(ae.ufp) AS numeric),2) ufp, round(CAST(avg(ae.ozone) AS numeric),2) ozone, round(CAST(avg(ae.rhumext) AS numeric),2) rhumext, round(CAST(avg(ae.tempext) AS numeric),2) tempext, round(CAST(avg(ae.no2) AS numeric),2) no2 ";
		var _from 		= " aireas_histecn ae, airbox a ";
		var _where 		= " 1=1 and ae.airbox || '.cal' = a.airbox ";
		var _groupBy	= " hist_year, a.airbox  ";
		var _orderBy	= _groupBy;
		
		var query = 'select ' + _attribute + ' from ' + _from + ' where ' + _where + 
		' group by ' + _groupBy + 
		' order by ' + _orderBy + ' ;';
		
		console.log('Postgres sql start execute: ' + query);
		executeSql(query, callback);

        return;
    }	


	
/*
-- jaar gemiddelde
SELECT extract(year from (tick_date - interval '1 hour')) jaar, to_number(airbox, '99') airbox, avg(pm1) pm1, avg(pm25) pm25, avg(pm10) pm10, avg(ufp) ufp, avg(ozone) ozone, avg(rhumext) rhumext, avg(tempext) tempext, avg(no2) no2
FROM public.aireas_histecn
group by jaar, airbox
order by jaar, airbox
;

-- maand gemiddelde
SELECT extract(year from (tick_date - interval '1 hour')) jaar, extract(month from (tick_date - interval '1 hour')) maand, to_number(airbox, '99') airbox, avg(pm1) pm1, avg(pm25) pm25, avg(pm10) pm10, avg(ufp) ufp, avg(ozone) ozone, avg(rhumext) rhumext, avg(tempext) tempext, avg(no2) no2
FROM public.aireas_histecn
group by jaar, maand, airbox
order by jaar, maand, airbox
;
*/





};


