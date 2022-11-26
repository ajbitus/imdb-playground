const https = require('node:https'),
  fs = require('node:fs'),
  path = require('node:path'),
  zlib = require('node:zlib'),
  stream = require('node:stream'),
  _ = require('lodash'),
  moment = require('moment'),
  _cliProgress = require('cli-progress'),
  chalk = require('chalk'),
  inquirer = require('inquirer');
const { promisify } = require('node:util');

// New method found to 'promisify' the 'https.get' method: https://gist.github.com/krnlde/797e5e0a6f12cc9bd563123756fc101f
https.get[promisify.custom] = function getAsync(options) {
  return new Promise((resolve, reject) => {
    https
      .get(options, (response) => {
        response.end = new Promise((resolve) => response.on('end', resolve));
        resolve(response);
      })
      .on('error', reject);
  });
};

const httpsGet = promisify(https.get);
const pipeline = promisify(stream.pipeline);

const folderBasePath = path.join(__dirname, '../..', '.dataset');

const confirmFilesLatest = async () => {
  if (fs.existsSync(folderBasePath)) {
    const latestFile = _.first(
      fs
        .readdirSync(folderBasePath)
        .filter((file) =>
          fs.lstatSync(path.join(folderBasePath, file)).isFile()
        )
        .map((file) => ({
          file,
          mtime: fs.lstatSync(path.join(folderBasePath, file)).mtime,
        }))
        .sort((a, b) => b.mtime.getTime() - a.mtime.getTime())
    );
    if (moment(latestFile.mtime).isBefore(moment().add(-7, 'days'))) {
      try {
        const answers = await inquirer.prompt([
          {
            type: 'confirm',
            name: 'promptOldDatasets',
            message:
              'Dataset files are older than a week. Do you want to download latest files?',
            default: false,
          },
        ]);
        return _.get(answers, 'promptOldDatasets', false);
      } catch (err) {
        console.error(chalk.bgRed(err));
        return false;
      }
    } else {
      try {
        const answers = await inquirer.prompt([
          {
            type: 'confirm',
            name: 'promptRedownloadDatasets',
            message:
              'Dataset files are new and downloaded in last 7 days. Do you want to download the files again?',
            default: false,
          },
        ]);

        return _.get(answers, 'promptRedownloadDatasets', false);
      } catch (err) {
        console.error(chalk.bgRed(err));
        return false;
      }
    }
  } else {
    return true;
  }
};

const download = async (url, filename) => {
  console.log(
    chalk.blueBright(`Downloading file "${filename}" from "${url}"...`)
  );

  const progressBar = new _cliProgress.SingleBar(
    {
      format: `{bar} {percentage}%`,
    },
    _cliProgress.Presets.shades_classic
  );

  let filePath = null,
    file,
    receivedBytes = 0;

  filePath = path.join(folderBasePath, filename);

  if (!fs.existsSync(folderBasePath)) {
    fs.mkdirSync(folderBasePath);
    chalk.blueBright(
      `Folder ".dataset" not found, created it in project root location...`
    );
  }

  if (fs.existsSync(filePath)) {
    fs.unlinkSync(filePath);
  }

  file = fs.createWriteStream(filePath);

  try {
    const downloadRequestResponse = await httpsGet(url, {
      accept: 'binary/octet-stream',
    });

    if (
      downloadRequestResponse.statusCode === 200 &&
      _.includes(downloadRequestResponse.headers['content-type'], 'stream')
    ) {
      const totalBytes = downloadRequestResponse.headers['content-length'];
      progressBar.start(totalBytes, receivedBytes);

      downloadRequestResponse.on('data', (chunk) => {
        receivedBytes += chunk.length;
        progressBar.update(receivedBytes);
      });

      try {
        await pipeline(downloadRequestResponse, file);
        progressBar.stop();
        setTimeout(() => {}, 1000);
        return null;
      } catch (err) {
        progressBar.stop();
        file.close();
        fs.unlinkSync(filePath);
        return err.message;
      }
    } else {
      file.close();
      fs.unlinkSync(filePath);
      return 'Fetch response invalid';
    }
  } catch (err) {
    // reject(err);
    console.log(
      chalk.bgRed(`Downloading failed for file "${filename}" from "${url}"...`)
    );
    console.error(err.statusCode);
    return err;
  }
};

const extract = async (sourceFilename, targetFilename) => {
  console.log(
    chalk.yellow(
      `Extracting from "${sourceFilename}" to "${targetFilename}"...`
    )
  );

  const progressBar = new _cliProgress.SingleBar(
    {
      format: `{bar} {percentage}%`,
    },
    _cliProgress.Presets.shades_classic
  );

  const sourceFilePath = path.join(
    __dirname,
    '../..',
    '.dataset',
    sourceFilename
  );

  const targetFilePath = path.join(
    __dirname,
    '../..',
    '.dataset',
    targetFilename
  );

  if (fs.existsSync(targetFilePath)) {
    fs.unlinkSync(targetFilePath);
  }

  const sourceFile = fs.createReadStream(sourceFilePath),
    targetFile = fs.createWriteStream(targetFilePath);

  let receivedBytes = 0,
    totalBytes = fs.statSync(sourceFilePath).size;

  progressBar.start(totalBytes, receivedBytes);

  sourceFile.on('data', (chunk) => {
    receivedBytes += chunk.length;
    progressBar.update(receivedBytes);
  });

  try {
    await pipeline(sourceFile, zlib.createGunzip(), targetFile);
    progressBar.update(totalBytes);
    progressBar.stop();
    return null;
  } catch (err) {
    progressBar.stop();
    fs.unlinkSync(targetFilePath);
    return 'Extraction failed!';
  }
};

const downloadAndExtract = async (files) => {
  if (await confirmFilesLatest()) {
    let fileCounter = 0;

    for (let i = 0; i < files.length; i++) {
      const file = files[i];
      try {
        const downloadError = await download(file.url, file.compressedFilename);
        if (downloadError) {
          console.error(chalk.bgRed(downloadError));
        } else {
          try {
            const extractError = await extract(
              file.compressedFilename,
              file.filename
            );
            if (extractError) {
              console.error(chalk.bgRed(extractError));
            } else {
              if (fs.existsSync(path.join(folderBasePath, file.filename))) {
                fileCounter++;
              }
            }
          } catch (err) {
            return err;
          }
        }
      } catch (err) {
        return err;
      }
    }

    if (fileCounter === files.length) {
      console.log(
        chalk.greenBright('Dataset download and extract successful!')
      );
    } else {
      console.error(
        chalk.bgRed(
          'Dataset listed and downloaded/extracted counts does not match!'
        )
      );
    }
  } else {
    console.error(
      chalk.yellowBright(
        'Dataset files already exists and user opted not to re-download!'
      )
    );
  }
};

module.exports = downloadAndExtract;
