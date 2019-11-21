const Db = require('./db.js');
const Global = require('./global.js');

(async function () {

	const item = {
		uid: 'a'
	};

	const option = {
		condition: {
			tid: 'tid'
		}
	};

	await Db.setup(Global);
	await Db.remove(item, option);

	console.log('REMOVE: should throw error');

}()).catch(console.error);
