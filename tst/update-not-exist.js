const Db = require('./db.js');
const Global = require('./global.js');

(async function() {

	await Db.setup(Global);

	await Db.update('sedb', {
		uid: 'z',
		text: 'hello wolrd'
	});

	console.log('\nUPDATE: not exist should throw');

}()).catch(console.error);
