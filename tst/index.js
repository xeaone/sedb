const Db = require('./db.js');
const Global = require('./global.js');

(async function() {

	await Db.setup(Global);

	result = await Db.add(table, {
		uid: 'a',
		gid: 'z',
		tid: 'users',
		number: 1,
		boolean: true,
		string: 'hello world'
	});
	
	console.log('\nADD');

	// result = await Db.add(table, {
	// 	uid: 'b',
	// 	gid: 'z',
	// 	tid: 'users',
	// 	number: 2,
	// 	boolean: true,
	// 	string: 'hello world'
	// });
	// console.log('\nADD');
	//
	// result = await Db.add(table, {
	// 	uid: 'c',
	// 	gid: 'z',
	// 	tid: 'users',
	// 	number: 3
	// });
	// console.log('\nADD');

	result = await Db.get(table, {
		uid: 'b'
	});
	console.log('\nGET: uid - ', result);

	result = await Db.update(table, {
		uid: 'b',
		boolean: !result.boolean
	});
	console.log('\nUPDATE: uid, boolean');

	result = await Db.query(table, {
		gid: 'z'
	});
	console.log('\nQUERY GSI: gid - ', result);

	result = await Db.query(table, {
		gid: 'z',
		tid: 'users',
		number: 3
	});
	console.log('\nQUERY GSI: gid, tid, number - ', result);

	result = await Db.remove(table, {
		uid: 'a'
	});
	console.log('\nREMOVE: uid - ', result);

}()).catch(console.error);
