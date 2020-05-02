require('dotenv').config();
const { MongoClient } = require('mongodb');
const { createWriteStream, readFile, unlink } = require('fs');
const AWS = require('aws-sdk');
const winston = require('winston');

const s3 = new AWS.S3({
    accessKeyId: process.env.AWS_ACCESS_KEY,
    secretAccessKey: process.env.AWS_ACCESS_SECRET
});

const NAMASTE_CONSTANTS = {
    TEST_DB: 'test',
    MAIN_DB: 'kukuhomeDB',
    COLLECTION_NAME: 'articles'
};

const URI = process.env.MONGO_URL;
const client = new MongoClient(URI, { useNewUrlParser: true, useUnifiedTopology: true });
const s3Params = { Bucket: process.env.S3_BUCKET_NAME };

const oldDate = new Date('2020-03-03T00:00:00.824Z').getTime();

const FileRecordThreshold = 1000;

const operate = async () => {
    try {
        const connection = await client.connect();
        const db = connection.db(NAMASTE_CONSTANTS.TEST_DB);
        const adminDb = db.admin();
        const databases = await adminDb.listDatabases();

        for (const database of databases.databases) {
            logger.info(`Database "${database.name}" found with size ${database.sizeOnDisk / (8 * 1024)} KBs.`);
        }

        const kukuhomeDB = connection.db(NAMASTE_CONSTANTS.MAIN_DB);
        const collections = await kukuhomeDB.listCollections().toArray();

        for (const collection of collections) {
            logger.info(`Collection - ${collection.name}`)
        }

        const collection = kukuhomeDB.collection(NAMASTE_CONSTANTS.COLLECTION_NAME);

        // const totalDocs = await collection.countDocuments();
        // logger.info(`Found a total of ${totalDocs} documents.`);
        // logger.info('Attempting to writing them into a file and getting rid of docs from DB.');

        let condition = true;
        let fileNumberIndex = 1;
        let recordsCount = 0;
        let isWritableStreamPresent = false;
        let writeStream;

        while (condition) {
            const fileName = `records_${fileNumberIndex}_E.ndjson`;
            const filePath = `files/${fileName}`;

            if (!isWritableStreamPresent) {
                writeStream = createWriteStream(filePath);
                isWritableStreamPresent = true;
            }

            const dataSample = await collection.find({}).sort({ creationDate: 1 }).limit(100).toArray();

            const docIds = [];
            for (const sampleLine of dataSample) {
                docIds.push(sampleLine._id);
                writeStream.write(JSON.stringify(sampleLine) + '\n');
                ++recordsCount;
                const currDate = new Date(sampleLine.cdate).getTime();
                condition = compareDates(currDate, oldDate);
            }

            // Delete old records
            const res = await collection.deleteMany({ _id: { $in: docIds } });
            logger.info(`Deleted ${res.result.n} records from DB.`);

            // count = await collection.countDocuments();
            // logger.info(`Remaining documents: ${count}`);

            if (recordsCount >= FileRecordThreshold || !condition) {
                writeStream.end(async (err) => {
                    if (err) throw err;
                    logger.info('Finished writing data to file.');
                });
                await uploadFileToS3(filePath, fileName);
                await deleteFile(filePath);
                ++fileNumberIndex;
                recordsCount = 0;
                isWritableStreamPresent = false;
            }
        }

        connection.close();
    } catch (err) {
        throw err;
    }
}

const uploadFileToS3 = async (filePath, fileName) => {
    return new Promise((res, rej) => {
        readFile(filePath, (err, data) => {
            if (err) return rej(err);
            s3Params.Key = fileName;
            s3Params.Body = data;
            s3.upload(s3Params, function (s3Err, data) {
                if (s3Err) return rej(s3Err);
                logger.info(`File uploaded successfully at ${data.Location}`);
                s3Params.Key = undefined;
                s3Params.Body = undefined;
                return res();
            });
        });
    });
}

const deleteFile = (filePath) => {
    return new Promise((res, rej) => {
        unlink(filePath, function (err) {
            if (err && err.code == 'ENOENT') {
                rej(new Error('File not found'));
            } else if (err) {
                rej(err);
            } else {
                logger.info(`Deleted file.`);
                res();
            }
        });
    });
}

const compareDates = (timeStamp1, timeStamp2) => {
    if (timeStamp1 <= timeStamp2) {
        return true;
    }
    return false;
}

const logger = winston.createLogger({
    level: 'info',
    transports: [
      new winston.transports.Console(),
      new winston.transports.File({ filename: 'logs/mongo-s3.log' })
    ]
  });

operate();