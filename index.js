const { showUserAndProject } = require('./utils/cli-print');
const { downloadAndExtract } = require('./modules/fetch-and-prepare-dataset');
const datasetList = require('./dataset');

(async (datasetList) => {
  await showUserAndProject();
  await downloadAndExtract(datasetList);
})(datasetList);
