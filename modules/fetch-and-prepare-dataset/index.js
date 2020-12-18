const https = require("https"),
  fs = require("fs"),
  path = require("path"),
  zlib = require("zlib"),
  _ = require("lodash"),
  _cliProgress = require("cli-progress"),
  chalk = require("chalk");
const { pipeline } = require("stream");

const download = async (url, filename) => {
  return await new Promise((resolve, reject) => {
    console.log(
      chalk.blueBright(`Downloading from "${filename}" from "${url}"...`)
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

    filePath = path.join(__dirname, "../..", ".dataset", filename);

    if (!fs.existsSync(path.join(__dirname, "../..", ".dataset"))) {
      reject("Filepath invalth, check your directories and set paths.");
    } else {
      if (fs.existsSync(filePath)) {
        fs.unlinkSync(filePath);
      }

      file = fs.createWriteStream(filePath);

      https
        .get(
          url,
          {
            accept: "binary/octet-stream",
          },
          (response) => {
            if (
              response.statusCode === 200 &&
              _.includes(response.headers["content-type"], "stream")
            ) {
              const totalBytes = response.headers["content-length"];
              progressBar.start(totalBytes, receivedBytes);

              pipeline(response, file, (err) => {
                if (err) {
                  progressBar.stop();
                  file.close();
                  fs.unlinkSync(filePath);
                  reject(err.message);
                } else {
                  progressBar.stop();
                  setTimeout(() => {}, 1000);
                  resolve(null);
                }
              });

              response.on("data", (chunk) => {
                receivedBytes += chunk.length;
                progressBar.update(receivedBytes);
              });
            } else {
              file.close();
              fs.unlinkSync(filePath);
              reject("Fetch response invalid");
            }
          }
        )
        .on("error", (err) => {
          reject(err);
        });
    }
  });
};

const extract = async (sourceFilename, targetFilename) => {
  return new Promise((resolve, reject) => {
    console.log(
      chalk.blue(
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
      "../..",
      ".dataset",
      sourceFilename
    );

    const targetFilePath = path.join(
      __dirname,
      "../..",
      ".dataset",
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

    sourceFile.on("data", (chunk) => {
      receivedBytes += chunk.length;
      progressBar.update(receivedBytes);
    });

    pipeline(sourceFile, zlib.createGunzip(), targetFile, (err) => {
      if (err) {
        progressBar.stop();
        fs.unlinkSync(targetFilePath);
        reject("Extraction failed!");
      } else {
        progressBar.update(totalBytes);
        progressBar.stop();
        resolve(null);
      }
    });
  });
};

const downloadAndExtract = async (files) => {
  let fileCounter = 0;

  for (let i = 0; i < files.length; i++) {
    const file = files[i];
    const downloadError = await download(
      file.url,
      file.compressedFilename
    ).catch((err) => {
      return err;
    });
    if (downloadError) {
      console.error(chalk.bgRed(downloadError));
    } else {
      const extractError = await extract(
        file.compressedFilename,
        file.filename
      ).catch((err) => {
        return err;
      });
      if (extractError) {
        console.error(chalk.bgRed(extractError));
      } else {
        if (
          fs.existsSync(
            path.join(__dirname, "../..", ".dataset", file.filename)
          )
        ) {
          fileCounter++;
        }
      }
    }
  }

  if (fileCounter === files.length) {
    console.log(chalk.green("Dataset download and extract successful!"));
  } else {
    console.error(
      chalk.bgRed(
        "Dataset listed and downloaded/extracted counts does not match!"
      )
    );
  }
};

module.exports = { downloadAndExtract: downloadAndExtract };
