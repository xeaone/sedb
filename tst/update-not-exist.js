const Db = require('./db.js');

(async function() {

	await Db.setup([{
		name: 'sedb',
		schema: [
			{
				hash: 'uid',
			},
			// {
			// 	gsi: 'tid',
			// 	hash: 'tid'
			// },
			{
				gsi: 'gid',
				hash: 'gid'
			}
		]
	}]);

	await Db.update('sedb', {
		uid: 'z',
		text: 'hello wolrd'
	});

	console.log('\nUPDATE: not exist should throw');

}()).catch(console.error);
