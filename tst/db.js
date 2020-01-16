const Sedb = require('../index.js');

const Db = new Sedb.dynamo({
    name: 'sedb',
    region: 'us-west-2',
    version: '2012-08-10',
    primary: { hash: 'uid' },
    indexes: [ { hash: 'gid' } ]
});

module.exports = Db;
