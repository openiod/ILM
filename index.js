
/**
 * OpenIoD module for connecting ILM sensor system 
 *
 * @param  {String} ##todo 
 * @return {String}
 */
module.exports = {


	loadAllModels: function(folder, ) {
	
		var modelLocalPath = folder + '/model/';
		var localModelIndex = -1;
		var localModels=[];
		fs.readdir(modelLocalPath, function (err, files) {
  			//localPostcodes=files;
  			//console.log("Local postcodes: " + localPostcodes.toString());
			if (err) { console.log("Local model folder not found: " + localmodels.toString());
			} else {
				localModels=files;
  				console.log("Local models: " + localModels.toString());
			}
		});
	}


};
