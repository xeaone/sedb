
const Sedb = require('../index.js');

(async function() {
	const table = 'sedb';

	const Db = new Sedb.dynamo({
        region: 'us-west-2',
		version: '2012-08-10'
    });

	await Db.setup([{
		name: table,
		schema: [
			{
				hash: 'uid',
			},
			{
				gsi: 'tid',
				hash: 'tid'
			},
			{
				gsi: 'gid',
				hash: 'gid'
			}
		]
	}]);

	result = await Db.put(table, {
		gid: 'z',
		uid: 'a',
		tid: 'users',
		number: 1,
		boolean: true,
		string: 'hello world'
	});
	console.log('\nPUT - ', result);

	result = await Db.put(table, {
		gid: 'z',
		uid: 'b',
		tid: 'users',
		number: 2,
		boolean: true,
		string: 'hello world'
	});
	console.log('\nPUT - ', result);

	result = await Db.get(table, {
		uid: 'b'
	});
	console.log('\nGET: uid - ', result);

	result = await Db.update(table, {
		uid: 'b',
		boolean: false
	});
	console.log('\nUPDATE: uid, boolean - ', result);

	result = await Db.query(table, {
		gid: 'z',
		number: 1
	});
	console.log('\nQUERY GSI: gid, number - ', result);

	result = await Db.query(table, {
		tid: 'users'
	});
	console.log('\nQUERY GSI: tid - ', result);

	result = await Db.remove(table, {
		uid: 'a'
	});
	console.log('\nREMOVE: uid - ', result);

}()).catch(console.error);
