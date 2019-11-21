const Sedb = require('../index.js');

const Db = new Sedb.dynamo({
    name: 'test-sedb',
    region: 'us-west-2',
	version: '2012-08-10'
});

module.exports = Db;
