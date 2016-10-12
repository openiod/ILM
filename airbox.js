
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
	client.connect(function(err,result) {
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
		callback(null, data);
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

/*
			var mapCallBackTest2 = function (data2) {
				console.log('test2');
				//mapCallBackTest(data2);
				setTimeout(mapCallBackTest, 10000, data2);
			//	mapCallBack2(null, data2);
			}
	//		console.log(setTimeout);
			mapCallBackTest2(data);
			//setTimeout(mapCallBackTest2, 1000, data);
*/
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



	getAireasAqi: function (param, callback) {
		
		if (sqlConnString == null) {
			this.initDbConnection({source:'postgresql', param: param });
		}
		
		var _attribute, _and1, _and2, _and3, _and4;
		var _attribute 	= " avg_type foi, to_char(aqi.retrieveddate AT TIME ZONE 'UTC' , 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') as isodatetime  , aqi.retrieveddate datetime,max(avg_aqi) aqi ";
		
		var _from 		= " public.grid_gem_foi_aqi aqi ";
		var _from2 		= " (select grid_code, avg_period, max(retrieveddate) retrieveddate from public.grid_gem_foi_aqi where date_part(\'minute\', retrieveddate) = 1 group by grid_code, avg_period) actual ";
		

		if (param.featureofinterest && param.featureofinterest != 'overall') {
			if (param.featureofinterest == 'all') {
				_and1 		= " ";
			} else {
				_and1 		= " and feature_of_interest = '" + param.featureofinterest + "' ";
			} 
		} else {
			 _and1 		= " and feature_of_interest = 'overall' ";
		}

		if (param.sensortype && param.sensortype != 'overall') {
			if (param.sensortype == 'all') {
				_and2 		= " ";
			} else {
				_and2 		= " and avg_type = '" + param.sensortype + "' ";
			} 
		} else {
			 _and2 		= " and avg_type = 'overall' ";
		}

//		console.log(param);
//		console.log(_and1);

		_and3 = ' and date_part(\'minute\', aqi.retrieveddate) = 1 \
 and aqi.avg_period = \'1hr\' \
 and actual.grid_code = aqi.grid_code \
 and actual.avg_period = aqi.avg_period ';

		_and4 = ' and aqi.grid_code = \'' + param.gridCode + '\' ';

		var _groupBy	= " avg_type, aqi.retrieveddate ";
		var _orderBy	= _groupBy;
		
		var query = 'select ' + _attribute + ' from ' + _from + ', ' + _from2 + ' where 1=1 ' + _and1 + _and2 + _and3 + _and4 +' group by ' + _groupBy + 
		' order by ' + _orderBy + ' ;';
		
		
		console.log('Postgres sql start execute: ' + query);
		executeSql(query, callback);

        return;
    },


	
	getAirboxDataAllAirboxes: function (param, callback) {
		var _attribute, _and;
		var _attribute 	= " identifier, airbox, airbox_type, airbox_location, airbox_location_desc, region, airbox_location_type, airbox_postcode, airbox_x, airbox_y, lat, lng ";
		var _from 		= " airbox a ";
		//var _where 		= " 1=1 ";
		//var _groupBy	= "  ";
		//var _orderBy	= _groupBy;
		var _orderBy = ' identifier ';
		
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
		' limit 10 ' +
		';' ;
		
		console.log('Postgres sql start execute: ' + query);
//		executeSqlStream(query, callback);
		executeSql(query, callback);

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
    },


	getCbsBuurtFromPoint: function (param, callback) {
		var query = 'select * from get_cbs_buurt_from_point(' + param.lng + ',' + param.lat + ');';
	
		console.log('Postgres sql start execute: ' + query);
		executeSql(query, callback);

        return;
    },


	getCbsBuurtNearestAirboxes: function (param, callback) {
		if (sqlConnString == null) {
			this.initDbConnection({source:'postgresql', param: param });
		};
		var query = "select ca.airbox, round(avg(ca.factor_distance)) avg_distance, max(airbox_location) airbox_location, ST_AsGeoJSON(ST_Simplify(max(geom),0.0001)) geojson \
			from grid_gem_cell c\
			, grid_gem_cell_airbox ca\
			, airbox ab \
			where 1=1 \
			and c.gid = ca.grid_gem_cell_gid \
			and c.bu_code = '" + param.bu_code + "' \
			and ca.airbox = ab.airbox \
			group by ca.airbox \
			order by ca.airbox";
	
		console.log('Postgres sql start execute: ' + query);
		executeSql(query, callback);

        return;
    },
	
		
	getCbsBuurtProjectEHVAirport: function (param, callback) {
		var query = '';
		if (sqlConnString == null) {
			this.initDbConnection({source:'postgresql', param: param });
		};
		
		
		
		if (param.objectId == 'geoLocationArea') {
			query = 'select gm_code, gm_naam, bu_code, bu_naam, ST_AsGeoJSON(ST_Simplify(geom4326,0.0001)) geojson from get_cbs_buurt_from_point(' + param.lng + ',' + param.lat + ');';
		} else {
		
			if (param.neighborhood == undefined) {
				param.neighborhood	= "\
	'BU08200000', 'BU08200001', 'BU08200002', 'BU08200003', 'BU08200008', 'BU08200009', 'BU08200100', 'BU08200109', 'BU08200200', 'BU08200209', \
	'BU17710000', 'BU17710001', 'BU17710002', 'BU17710003', 'BU17710004', 'BU17710005', 'BU17710006', 'BU17710007', 'BU17710009', 'BU17710100', 'BU17710109', 'BU17240300', 'BU17240301', 'BU17240309', \
	'BU07721633', 'BU07721634', 'BU07721635', 'BU07721639', 'BU07721640',\
	'BU07530001', 'BU07530002', 'BU07530003', 'BU07530004', 'BU07530005', 'BU07530006', 'BU07530007', 'BU07530008', 'BU07530009', 'BU07530010', 'BU07530011', 'BU07530012', 'BU07530013', 'BU07530014', 'BU07530015', 'BU07530016', 'BU07530017'" +
	",'BU04390402','BU03630668'";  //gors-noord en nieuwendam voor testen 
			}
			
			query = 'select bu_code, gm_code, gm_naam, bu_naam, ST_AsGeoJSON(ST_Simplify(geom4326,0.0001)) geojson from cbsbuurt2012 where bu_code in (' + param.neighborhood + ');';
		}
	      
		console.log('Postgres sql start execute: ' + query);
		executeSql(query, callback);

        return;
    },


	getProjectAirportData: function (param, callback) {
		if (sqlConnString == null) {
			this.initDbConnection({source:'postgresql', param: param });
		};
		
		var _startDate	= '2016-06-01';
		var _endDate	= '2016-07-31';
		if (param.query.startdate) {
			_startDate = param.query.startdate;
		}
		if (param.query.enddate) {
			_endDate = param.query.enddate;
		}
		
		var _foiCode		= '';
		var _andDeviceIds	= '';
		var _andFoiCodes	= '';		
		if (param.query.foicode) {
			_foiCode = param.query.foicode;
			_andDeviceIds	= ' and device_id in (' +  _foiCode + ') '; 
			_andFoiCodes	= ' and foi_code in (' +  _foiCode + ') '; 
		}

		var _sensorName		= '';
		var _andSensorNames	= '';
		if (param.query.sensorname) {
			_sensorName = param.query.sensorname;
			_andSensorNames	= ' and sensor_name in (' +  _sensorName + ') '; 
		}

		
		var _deviceIds = _foiCode
		
		
		
		var query 		= 'select * from (';
		var queryEvent	= '';
		var queryJose	= '';
		var queryAera	= '';
		
		if (param.query.source) {
			var _source = param.query.source.split(',');
			for (var i=0;i<_source.length;i++) {
				if (_source[i]=='event') {
					queryEvent = "select foi_code foi, to_char(event_date AT TIME ZONE 'UTC' , 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') as date \
		, null sensorvalue, event_desc as event, event_remarks remarks, null observations, lat, lng, null z \
from aera_import_event aee \
where 1=1 \
and event_date >= '" + _startDate + "' \
and event_date <= '" + _endDate + "' " +
_andFoiCodes + 
" --where aee.foi_code = 'ww148e' ";
				}

				if (_source[i]=='jose') {
					queryJose = "select device_id, to_char(measurement_date AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), sensor_value, sensor_label,sensor_unit || ' avg per hour', sample_count, lat,lng, altitude \
from intemo_import ii \
where 1=1 \
and measurement_date >= '" + _startDate + "' \
and measurement_date <= '" + _endDate + "' " +
_andSensorNames +
_andDeviceIds; 
				}

				if (_source[i]=='josene-detail') {
					queryJose = "select device_id, to_char(date_trunc('minute', measurement_date AT TIME ZONE 'UTC'), 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), round(avg(v_audio_9)), 'v_audio_9','v_audio_9 avg during base timer interval', count(*), null,null,null \
from intemo_detail_import ii \
where 1=1 \
and v_audio_9 is not null \
and measurement_date >= '" + _startDate + "' \
and measurement_date <= '" + _endDate + "' " +
_andSensorNames +
_andDeviceIds+
" group by device_id, date_trunc('minute', measurement_date AT TIME ZONE 'UTC') "; 

					queryJose += "UNION select device_id, to_char(date_trunc('minute', measurement_date AT TIME ZONE 'UTC'), 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), round(avg(v_audio_8)), 'v_audio_8','v_audio_8 avg during base timer interval', count(*), null,null,null \
from intemo_detail_import ii \
where 1=1 \
and v_audio_8 is not null \
and measurement_date >= '" + _startDate + "' \
and measurement_date <= '" + _endDate + "' " +
_andSensorNames +
_andDeviceIds+
" group by device_id, date_trunc('minute', measurement_date AT TIME ZONE 'UTC') "; 
					queryJose += "UNION select device_id, to_char(date_trunc('minute', measurement_date AT TIME ZONE 'UTC'), 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), round(avg(v_audio_7)), 'v_audio_7','v_audio_7 avg during base timer interval', count(*), null,null,null \
from intemo_detail_import ii \
where 1=1 \
and v_audio_7 is not null \
and measurement_date >= '" + _startDate + "' \
and measurement_date <= '" + _endDate + "' " +
_andSensorNames +
_andDeviceIds+
" group by device_id, date_trunc('minute', measurement_date AT TIME ZONE 'UTC') "; 
					queryJose += "UNION select device_id, to_char(date_trunc('minute', measurement_date AT TIME ZONE 'UTC'), 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), round(avg(v_audio_6)), 'v_audio_6','v_audio_6 avg during base timer interval', count(*), null,null,null \
from intemo_detail_import ii \
where 1=1 \
and v_audio_6 is not null \
and measurement_date >= '" + _startDate + "' \
and measurement_date <= '" + _endDate + "' " +
_andSensorNames +
_andDeviceIds+
" group by device_id, date_trunc('minute', measurement_date AT TIME ZONE 'UTC') "; 
					queryJose += "UNION select device_id, to_char(date_trunc('minute', measurement_date AT TIME ZONE 'UTC'), 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), round(avg(v_audio_5)), 'v_audio_5','v_audio_5 avg during base timer interval', count(*), null,null,null \
from intemo_detail_import ii \
where 1=1 \
and v_audio_5 is not null \
and measurement_date >= '" + _startDate + "' \
and measurement_date <= '" + _endDate + "' " +
_andSensorNames +
_andDeviceIds+
" group by device_id, date_trunc('minute', measurement_date AT TIME ZONE 'UTC') "; 
					queryJose += "UNION select device_id, to_char(date_trunc('minute', measurement_date AT TIME ZONE 'UTC'), 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), round(avg(v_audio_4)), 'v_audio_4','v_audio_4 avg during base timer interval', count(*), null,null,null \
from intemo_detail_import ii \
where 1=1 \
and v_audio_4 is not null \
and measurement_date >= '" + _startDate + "' \
and measurement_date <= '" + _endDate + "' " +
_andSensorNames +
_andDeviceIds+
" group by device_id, date_trunc('minute', measurement_date AT TIME ZONE 'UTC') "; 
					queryJose += "UNION select device_id, to_char(date_trunc('minute', measurement_date AT TIME ZONE 'UTC'), 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), round(avg(v_audio_3)), 'v_audio_3','v_audio_3 avg during base timer interval', count(*), null,null,null \
from intemo_detail_import ii \
where 1=1 \
and v_audio_3 is not null \
and measurement_date >= '" + _startDate + "' \
and measurement_date <= '" + _endDate + "' " +
_andSensorNames +
_andDeviceIds+
" group by device_id, date_trunc('minute', measurement_date AT TIME ZONE 'UTC') "; 
					queryJose += "UNION select device_id, to_char(date_trunc('minute', measurement_date AT TIME ZONE 'UTC'), 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), round(avg(v_audio_2)), 'v_audio_2','v_audio_2 avg during base timer interval', count(*), null,null,null \
from intemo_detail_import ii \
where 1=1 \
and v_audio_2 is not null \
and measurement_date >= '" + _startDate + "' \
and measurement_date <= '" + _endDate + "' " +
_andSensorNames +
_andDeviceIds+
" group by device_id, date_trunc('minute', measurement_date AT TIME ZONE 'UTC') "; 

					queryJose += "UNION select device_id, to_char(date_trunc('minute', measurement_date AT TIME ZONE 'UTC'), 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), round(avg(v_audio_1)), 'v_audio_1','v_audio_1 avg during base timer interval', count(*), null,null,null \
from intemo_detail_import ii \
where 1=1 \
and v_audio_1 is not null \
and measurement_date >= '" + _startDate + "' \
and measurement_date <= '" + _endDate + "' " +
_andSensorNames +
_andDeviceIds+
" group by device_id, date_trunc('minute', measurement_date AT TIME ZONE 'UTC') "; 

					queryJose += "UNION select device_id, to_char(date_trunc('minute', measurement_date AT TIME ZONE 'UTC'), 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), round(avg(v_audio_0)), 'v_audio_0','v_audio_0 avg during base timer interval', count(*), null,null,null \
from intemo_detail_import ii \
where 1=1 \
and v_audio_0 is not null \
and measurement_date >= '" + _startDate + "' \
and measurement_date <= '" + _endDate + "' " +
_andSensorNames +
_andDeviceIds +
" group by device_id, date_trunc('minute', measurement_date AT TIME ZONE 'UTC') "; 

				}


				if (_source[i]=='aeraH') {
					queryAera = "select foi_code, to_char(date_trunc('hour', measurement_date AT TIME ZONE 'UTC'), 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), round(avg(n)), 'UFP(H)','particles/cm^3 avg per hour', count(*), max(lat), max(lng), null  \
from aera_import ae \
where 1=1 \
and measurement_date >= '" + _startDate + "' \
and measurement_date <= '" + _endDate + "' "  +
_andFoiCodes + 
" group by foi_code, date_trunc('hour', measurement_date AT TIME ZONE 'UTC') ";
				}

				if (_source[i]=='aeraM') {
					queryAera = "select foi_code, to_char(date_trunc('minute', measurement_date AT TIME ZONE 'UTC'), 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), round(avg(n)), 'UFP(M)','particles/cm^3 avg per minute', count(*), max(lat), max(lng), null  \
from aera_import ae \
where 1=1 \
and measurement_date >= '" + _startDate + "' \
and measurement_date <= '" + _endDate + "' "  +
_andFoiCodes + 
" group by foi_code, date_trunc('minute', measurement_date AT TIME ZONE 'UTC') ";
				}

				if (_source[i]=='aera') {
					queryAera = "select foi_code, to_char(measurement_date AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), n, 'UFP','particles/cm^3 per 10 sec', 1, lat, lng, null  \
from aera_import ae \
where 1=1 \
and measurement_date >= '" + _startDate + "' \
and measurement_date <= '" + _endDate + "' "  +
_andFoiCodes + 
" ";
				}



			}
		}
		
		// to be sure the columns contain the correct labels (first select in UNION)
		if (queryEvent == '') {  
			queryEvent = "select foi_code foi, to_char(event_date AT TIME ZONE 'UTC' \
		, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') as date, null sensorvalue, event_desc as event, event_remarks remarks, null observations, lat, lng, null z \
from aera_import_event aee \
where 1=2 ";
		}
		
		
/*		
	select * from 
( select foi_code foi, event_date date, null sensorvalue, event_desc as event, event_remarks remarks, null observations, lat, lng 
from aera_import_event aee
where aee.foi_code = 'ww148e'
UNION 
select foi_code, date_trunc('hour', measurement_date), round(avg(n)), 'UFP(H)','particles/cm^3 avg per hour', count(*), max(lat), max(lng)  
from aera_import ae
where ae.foi_code = 'ww148e'
and measurement_date >= '2016-06-22 15:00:00+02'
and measurement_date <= '2016-06-26 23:00:00+02'
group by foi_code, date_trunc('hour', measurement_date)
UNION 
select foi_code, date_trunc('minute', measurement_date), round(avg(n)), 'UFP(M)','particles/cm^3 avg per minute', count(*), max(lat), max(lng)  
from aera_import ae
where ae.foi_code = 'ww148e'
and measurement_date >= '2016-06-22 15:00:00+02'
and measurement_date <= '2016-06-26 23:00:00+02'
group by foi_code, date_trunc('minute', measurement_date)
UNION
select device_id, measurement_date, sensor_value, sensor_label,sensor_unit || ' avg per hour', sample_count, lat,lng 
from intemo_import ii
where 1=1
and measurement_date >= '2016-06-22 15:00:00+02'
and measurement_date <= '2016-06-26 23:00:00+02'
and sensor_name = 'noiseavg'
and device_id = '43'
) tt
order by date
;	
		
*/		
/*
		var query = "select foi_code foi, to_char(event_date \
		, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') as date, null sensorvalue, event_desc as event, event_remarks remarks, null observations, lat, lng \
from aera_import_event aee \
--where aee.foi_code = 'ww148e' \
order by date";
*/
		
/*
		var query = "select ca.airbox, round(avg(ca.factor_distance)) avg_distance, max(airbox_location) airbox_location, ST_AsGeoJSON(ST_Simplify(max(geom),0.0001)) geojson \
			from grid_gem_cell c\
			, grid_gem_cell_airbox ca\
			, airbox ab \
			where 1=1 \
			and c.gid = ca.grid_gem_cell_gid \
			and c.bu_code = '" + param.bu_code + "' \
			and ca.airbox = ab.airbox \
			group by ca.airbox \
			order by ca.airbox";
*/


		query	+=  queryEvent; // always filled, default is dummy select for column labels needed for UNION
		if (queryJose != '') {
			query += '\n UNION ' + queryJose; 
		}
		
		if (queryAera != '') {
			query += '\n UNION ' + queryAera; 
		}
		
		query += '\n ) result order by date; ';
			
					
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


