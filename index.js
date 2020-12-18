const { showUserAndProject } = require("./utils/cli-print");
const { downloadAndExtract } = require("./modules/fetch-and-prepare-dataset");
const datasetList = require("./dataset");

showUserAndProject();
downloadAndExtract(datasetList);
