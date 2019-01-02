const Db = require('./db.js');
const Global = require('./global.js');

(async function() {

	const item = {
		uid: 'a',
		gid: 'z',
		tid: 'users',
		number: 1,
		boolean: true,
		string: 'hello world'
	};

	await Db.setup(Global);
	await Db.add('sedb', item);

	console.log('ADD: should add item with uid=a');

}()).catch(console.error);
