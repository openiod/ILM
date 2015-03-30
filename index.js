
/**
 * OpenIoD module for connecting ILM sensor system 
 *
 * @param  {String} ##todo 
 * @return {String}
 */
 
var fs 		= require('fs');
var request = require('request');
var sys 	= require('sys');
 
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
	
	reqCsvHistory: function (options) {

		airboxCsvFileName 		= '25_cal.csv';

		aireasLocalPathRoot = __dirname + '/../../';
//		fileFolder 			= '';
//		tmpFolder 			= aireasLocalPathRoot + fileFolder + "/" + 'tmp/';
		tmpFolder 			= aireasLocalPathRoot;

		// create subfolders
//		try {fs.mkdirSync(tmpFolder );} catch (e) {};//console.log('ERROR: no tmp folder found, batch run aborted.'); return } ;

		// 10-minuten reeksen met actuele AiREAS luchtmetingen. Verversing elke 10 minuten.
	
		this.streamCsvHistoryFile (csvHistoryUrl + airboxCsvFileName, airboxCsvFileName,	false, 'aireascsvdata');

		console.log('All retrieve actions are activated.');

	},
	
	streamCsvHistoryFile: function (url, fileName, unzip, desc) {
	
		var _wfsResult=null;
		console.log("Request start: " + desc + " (" + url + ")");


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

			writeFile(tmpFolder, fileName, iso8601 + ' ' + _wfsResult);
			})
  		);

	} // end of reqFile	
	



};




