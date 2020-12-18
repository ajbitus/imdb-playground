const fs = require("fs");
const path = require("path");
const readline = require("readline");
const chalk = require("chalk");

const printTextFileWithColor = (filePath, colorHexCode) => {
  chalk.reset();
  const readInterface = readline.createInterface({
    input: fs.createReadStream(path.join(__dirname, filePath)),
    console: false,
  });

  readInterface.on("line", function (line) {
    console.log(chalk.hex(colorHexCode)`${line}`);
  });
};

const showUserAndProject = () => {
  printTextFileWithColor("user-logo.txt", "#0088AA");
  printTextFileWithColor("project-logo.txt", "#f3ce13");
};

module.exports = { showUserAndProject: showUserAndProject };
