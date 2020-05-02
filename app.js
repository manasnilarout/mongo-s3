const { MongoClient } = require('mongodb');
const { createWriteStream } = require('fs');

const URI = "";
const client = new MongoClient(URI, { useNewUrlParser: true, useUnifiedTopology: true });
const FileRecordThreshold = 100;

const operate = async () => {
    try {
        const connection = await client.connect();
        const db = connection.db('test');
        const adminDb = db.admin();
        const databases = await adminDb.listDatabases();

        for (const database of databases.databases) {
            logger(`Database "${database.name}" found with size ${database.sizeOnDisk / (8 * 1024)} KBs.`);
        }

        const airBnb = connection.db('sample_airbnb');
        const collections = await airBnb.listCollections().toArray();

        for (const collection of collections) {
            logger(`Collection - ${collection.name}`)
        }

        const collection = airBnb.collection('listingsAndReviews');
        const totalDocs = await collection.countDocuments();

        logger(`Found a total of ${totalDocs} documents.`);
        logger('Attempting to writing them into a file and getting rid of docs from DB.');

        let count = true;
        let fileNumberIndex = 1;
        let recordsCount = 0;

        while (count) {
            const dataSample = await collection.find({}).sort({ last_review: 1 }).limit(50).toArray();

            const writeStream = createWriteStream(`files/records_${fileNumberIndex}.ndjson`);

            const docIds = [];
            for (const sampleLine of dataSample) {
                docIds.push(sampleLine._id);
                writeStream.write(JSON.stringify(sampleLine));
                ++recordsCount;
            }

            const res = await collection.deleteMany({ _id: { $in: docIds } });
            logger(`Deleted ${res.result.n} records from DB.`);

            count = await collection.countDocuments();
            logger(`Remaining documents: ${count}`);

            if (recordsCount >= FileRecordThreshold) {
                writeStream.end((err) => {
                    if (err) throw err;
                    logger('Finished writing data to file.');
                });
                ++fileNumberIndex;
                recordsCount = 0;
            }
        }

        connection.close();
    } catch (err) {
        throw err;
    }
}

const logger = (text) => {
    const time = new Date().toISOString();
    console.log(`${time} : ${text}`);
}

operate();