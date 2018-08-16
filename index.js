'use strict';

const S3 = require('./lib/s3.js');
const Dynamo = require('./lib/dynamo.js');

module.exports = {
	s3: S3,
	dynamo: Dynamo
};
