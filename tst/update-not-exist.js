const Db = require('./db.js');
const Global = require('./global.js');

(async function () {

	const item = {
		uid: 'z',
		text: 'hello wolrd'
	};

	await Db.setup(Global);
	await Db.update(item);

	console.log('UPDATE: not exist should throw');

}()).catch(console.error);
