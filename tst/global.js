
module.exports = [{
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
}];
