
const Sedb = require('../index.js');

(async function() {
	const table = 'sedb';

	const Db = new Sedb.dynamo({
        region: 'us-west-2',
		version: '2012-08-10'
    });

	await Db.setup([{
		name: table,
		index: true
	}]);

	result = await Db.put(table, {
		gid: 'z',
		uid: 'a',
		number: 1,
		boolean: true,
		string: 'hello world'
	});

	console.log('\n\nPUT - ', result);

	result = await Db.put(table, {
		gid: 'z',
		uid: 'b',
		number: 2,
		boolean: true,
		string: 'hello world'
	});

	console.log('\n\nPUT - ', result);

	result = await Db.get(table, {
		gid: 'z',
		uid: 'b',
	});

	console.log('\n\nGET - ', result);

	result = await Db.update(table, {
		gid: 'z',
		uid: 'b',
		number: 1,
		boolean: false
	});

	console.log('\n\nUPDATE - ', result);

	result = await Db.query(table, {
		gid: 'z',
		number: 1
	});

	console.log('\n\nQUERY - ', result);

	result = await Db.query(table, {
		uid: 'b'
	});

	console.log('\n\nQUERY INDEX - ', result);

	result = await Db.remove(table, {
		gid: 'z',
		uid: 'b'
	});

	console.log('\n\nREMOVE - ', result);

}()).catch(function (error) {
	console.error(error);
});
