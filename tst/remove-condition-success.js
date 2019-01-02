const Db = require('./db.js');
const Global = require('./global.js');

(async function() {

	const item = {
		uid: 'a'
	};

	const option = {
		condition: {
			tid: 'users'
		}
	};

	await Db.setup(Global);
	await Db.remove('sedb', item, option);

	console.log('REMOVE: should remove item');

}()).catch(console.error);
