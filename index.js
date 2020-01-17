const Dynamo = require('./lib/dynamo.js');
const S3 = require('./lib/s3.js');

module.exports = {
    s3: S3,
    dynamo: Dynamo
};
