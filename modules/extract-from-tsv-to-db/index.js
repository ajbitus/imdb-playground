const _ = require('lodash'),
  fs = require('node:fs'),
  path = require('node:path'),
  stream = require('node:stream'),
  _cliProgress = require('cli-progress'),
  chalk = require('chalk'),
  inquirer = require('inquirer');
const { parse } = require('csv-parse');
const { MongoClient } = require('mongodb');
const { promisify } = require('node:util');

const getLinesCountFromFile = require('../../utils/getLinesCountFromFile');

const pipeline = promisify(stream.pipeline);

const folderBasePath = path.join(__dirname, '../..', '.dataset');

const client = new MongoClient('mongodb://127.0.0.1/imdb-playground', {
  writeConcern: 'majority',
  retryWrites: true,
});

const db = client.db('imdb-playground');

const confirmIfCollectionsExist = async (datasetList) => {
  const collections = await db.collections();
  let isRecordsExist = false;

  for (let i = 0; i < collections.length; i++) {
    if (
      _.indexOf(_.map(datasetList, 'name'), collections[i].collectionName) >
        -1 ||
      _.indexOf(
        _.map(datasetList, 'name'),
        _.replace(collections[i].collectionName, 'temp.', '')
      ) > -1
    ) {
      const recordsCount = await collections[i].countDocuments();

      if (recordsCount > 0) {
        isRecordsExist = true;
      }
    }
  }

  if (isRecordsExist) {
    try {
      const answers = await inquirer.prompt([
        {
          type: 'confirm',
          name: 'promptCheckRecordsInDB',
          message:
            'Seems records are already available in database. Do you want to erase all data in the database and insert the data again?',
          default: false,
        },
      ]);

      const answer = _.get(answers, 'promptCheckRecordsInDB', false);

      if (answer) {
        await db.dropDatabase();
      }

      return answer;
    } catch (err) {
      console.error(chalk.bgRed(err));
      return false;
    }
  }

  return !isRecordsExist;
};

const readFromTSVToDBRaw = async (dataset, filePath) => {
  const collection = db.collection(`temp.${dataset.name}`);

  try {
    await collection.deleteMany({});
  } catch (err) {
    console.error(err);
    return `Deleting all records from collection "${collection.collectionName}" failed!`;
  }

  const totalRecords = (await getLinesCountFromFile(filePath)) - 1;

  console.log(
    chalk.blueBright(
      `Inserting raw records from "${dataset.filename}" to "temp.${dataset.name}" collection...`
    )
  );

  const progressBar = new _cliProgress.SingleBar(
    {
      format: `{bar} {percentage}%`,
    },
    _cliProgress.Presets.shades_classic
  );

  const fileReadStream = fs.createReadStream(filePath, {
    highWaterMark: 1,
  });

  const parser = parse({
    delimiter: '\t',
    columns: true,
    relax_quotes: true,
    escape: '\\',
    ltrim: true,
    rtrim: true,
  });

  let recordsInserted = 0;

  parser.on('data', async (record) => {
    try {
      await collection.insertOne(record);
      recordsInserted++;
      progressBar.update(recordsInserted);
    } catch (err) {
      parser.end();
      fileReadStream.close();
      return err.message;
    }
  });

  parser.on('error', (err) => {
    parser.end();
    fileReadStream.close();
    return err.message;
  });

  fileReadStream.on('error', (err) => {
    parser.end();
    fileReadStream.close();
    return err.message;
  });

  progressBar.start(totalRecords, recordsInserted);

  try {
    await pipeline(fileReadStream, parser);
    progressBar.update(totalRecords);
  } catch (err) {
    parser.end();
    fileReadStream.close();
    return err.message;
  }

  progressBar.stop();

  return null;
};

const checkRecordCounts = async (datasetList) => {
  const collections = await db.collections();
  let isRecordsExist = true;

  for (let i = 0; i < collections.length; i++) {
    const filePath = path.join(
      folderBasePath,
      _.get(datasetList[i], 'filename', '')
    );

    const totalRecords = (await getLinesCountFromFile(filePath)) - 1;

    if (
      _.indexOf(_.map(datasetList, 'name'), collections[i].collectionName) >
        -1 ||
      _.indexOf(
        _.map(datasetList, 'name'),
        _.replace(collections[i].collectionName, 'temp.', '')
      ) > -1
    ) {
      const recordsCount = await collections[i].countDocuments();

      if (totalRecords !== recordsCount) {
        isRecordsExist = false;
      }
    }
  }

  return _.len(datasetList) <= _.len(collections) ? isRecordsExist : false;
};

const extractFromTSVToDB = async (datasetList) => {
  if (await confirmIfCollectionsExist(datasetList)) {
    try {
      await client.connect();

      client.on('open', () => {
        console.log(chalk.greenBright('Database connection open!'));
      });

      client.on('close', () => {
        console.log(chalk.yellowBright('Database connection closed!'));
      });

      for (let i = 0; i < datasetList.length; i++) {
        const filePath = path.join(
          folderBasePath,
          _.get(datasetList[i], 'filename', '')
        );
        const errorMessage = await readFromTSVToDBRaw(
          datasetList[i],
          filePath,
          client
        );

        if (_.isString(errorMessage)) {
          console.error(chalk.bgRed(errorMessage));
        }
      }
    } catch (err) {
      console.error(chalk.bgRed(err.message));
    }
  } else {
    console.error(
      chalk.yellowBright(
        'Records already exists in the database and user opted not to populated the data!'
      )
    );
  }

  if (await checkRecordCounts(datasetList)) {
    console.log(
      chalk.greenBright('Records extraction and database seeding successful!')
    );
  } else {
    console.error(
      chalk.bgRed(
        'Records in database collections and source files counts does not match!'
      )
    );
  }

  await client.close();
};

module.exports = extractFromTSVToDB;
