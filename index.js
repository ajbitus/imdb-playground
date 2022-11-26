// const showUserAndProject = require('./utils/cli-print');
// const downloadAndExtract = require('./modules/download-and-extract');
const extractFromTSVToDB = require('./modules/extract-from-tsv-to-db');
const datasetList = require('./datasets');

(async (datasetList) => {
  // await showUserAndProject();
  // await downloadAndExtract(datasetList);
  await extractFromTSVToDB(datasetList);
})(datasetList);
