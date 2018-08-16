
const Sedb = require('../index.js');

(async function() {
	const table = 'sedb';

	const Db = new Sedb.dynamo({
        region: 'us-west-2'
    });

	await Db.setup({
		tables: [ table ]
	});

	result = await Db.put({
		gid: 'z',
		uid: 'a',
		body: {
			number: 1,
			boolean: true,
			string: 'hello world'
		},
		table: table
	});

	console.log('\n\nPUT - ', result);

	result = await Db.put({
		gid: 'z',
		uid: 'b',
		body: {
			number: 2,
			boolean: true,
			string: 'hello world'
		},
		table: table
	});

	console.log('\n\nPUT - ', result);


	result = await Db.get({
		gid: 'z',
		uid: 'b',
		table: table
	});

	console.log('\n\nGET - ', result);

	result = await Db.update({
		gid: 'z',
		uid: 'b',
		body: {
			number: 1,
			boolean: false
		},
		table: table
	});

	console.log('\n\nUPDATE - ', result);

	result = await Db.query({
		gid: 'z',
		body: {
			number: 1
		},
		table: table
	});

	console.log('\n\nQUERY - ', result);

	result = await Db.remove({
		gid: 'z',
		uid: 'b',
		table: table
	});

	console.log('\n\nREMOVE - ', result);

}()).catch(function (error) {
	console.error(error);
});
