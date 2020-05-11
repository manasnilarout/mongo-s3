require('dotenv').config();
const csv = require("csvtojson");
const { MongoClient } = require('mongodb');
const { createReadStream } = require('fs');
const winston = require('winston');

const URI = process.env.MONGO_URL;
const client = new MongoClient(URI, { useNewUrlParser: true, useUnifiedTopology: true });

const NAMASTE_CONSTANTS = {
    TEST_DB: 'test',
    MAIN_DB: 'kukuhomeDB',
    COLLECTION_NAME: 'articles'
};

const run = async () => {
    try {
        // Create a readStream of file to be inserted. (Must be a CSV with proper schema)
        const readStream = createReadStream('../source/bb417c5f-59f4-401b-86bb-13dced27f2ce.csv');

        // Establish connection with mongo server
        const connection = await client.connect();
        const db = connection.db(NAMASTE_CONSTANTS.TEST_DB);
        const kukuhomeDB = connection.db(NAMASTE_CONSTANTS.MAIN_DB);
        logger.info('Connected to Mongo server and found database.');
        const collection = kukuhomeDB.collection(NAMASTE_CONSTANTS.COLLECTION_NAME);

        csv()
            .fromStream(readStream)
            .subscribe(function (jsonObj) {
                return new Promise(async (res, rej) => {
                    try {
                    const result = await collection.insertOne(jsonObj);
                    // console.log(result);
                    logger.info('Inserted records successfully to DB.');
                    res();
                    } catch (e) {
                        logger.error('There was an error while data insertion to DB.', e);
                        res();
                    }
                });
            })
    } catch (err) {
        logger.error('There was an error while attempting to write data to DB.', err);
    }
}

const logger = winston.createLogger({
    level: 'info',
    transports: [
        new winston.transports.Console(),
        new winston.transports.File({ filename: '../logs/mongo-s3.log' })
    ]
});

run();