const Db = require('./db.js');
const Global = require('./global.js');

(async function() {

	const item = {
		uid: 'a'
	};

	await Db.setup(Global);
	await Db.remove('sedb', item);

	console.log('REMOVE: should remove item with uid=a');

}()).catch(console.error);
