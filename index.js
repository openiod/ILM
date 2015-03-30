
/**
 * OpenIoD module for connecting ILM sensor system 
 *
 * @param  {String} ##todo 
 * @return {String}
 */
 
var fs 		= require('fs');
 
module.exports = {


	loadAllModels: function(folder) {
	
		console.log('dirname: '+ __dirname);
		console.log('folder: '+ folder);
	
//		var modelLocalPath = folder + '/model/';
		var modelLocalPath = __dirname+'/model/';
		var localModelIndex = -1;
		var localModels=[];
		fs.readdir(modelLocalPath, function (err, files) {
  			//localPostcodes=files;
  			//console.log("Local postcodes: " + localPostcodes.toString());
			if (err) { console.log("Local model folder not found: " + modelLocalPath);
			} else {
				localModels=files;
  				console.log("Local models: " + localModels.toString());
			}
		});
	}


};
