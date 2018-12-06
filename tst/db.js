const Sedb = require('../index.js');

module.exports = new Sedb.dynamo({
    region: 'us-west-2',
	version: '2012-08-10'
});
