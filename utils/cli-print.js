const fs = require('fs');
const path = require('path');
const readline = require('readline');
const chalk = require('chalk');

const printTextFileWithColor = async (filePath, colorHexCode) => {
  chalk.reset();
  const readInterface = readline.createInterface({
    input: fs.createReadStream(path.join(__dirname, filePath)),
    console: false,
  });

  for await (const line of readInterface) {
    console.log(chalk.hex(colorHexCode)`${line}`);
  }
};

const showUserAndProject = async () => {
  await printTextFileWithColor('user-logo.txt', '#0088AA');
  await printTextFileWithColor('project-logo.txt', '#f3ce13');
};

module.exports = { showUserAndProject: showUserAndProject };
