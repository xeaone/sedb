'use stric';

const Aws = require('aws-sdk');

module.exports = class Dynamo {

	constructor (data) {
		const self = this;

		data = data || {};

		self.read = data.read || 1,
		self.write = data.write || 1;

		self.input = Aws.DynamoDB.Converter.input;
		self.output = Aws.DynamoDB.Converter.output;

		self.marshall = Aws.DynamoDB.Converter.marshall;
		self.unmarshall = Aws.DynamoDB.Converter.unmarshall;

		const options = data.options || {
			region: data.region,
			apiVersion: data.version
		};

		self.dynamo = new Aws.DynamoDB(options);
	}

	async setup (data) {
		const self = this;

		data = data || {};

		if (!data.tables) throw new Error('tables array required');
		if (!data.tables.length) throw new Error('table names required');

		const options = {
			KeySchema: [
				{
					AttributeName: 'gid',
					KeyType: 'HASH'
				},
				{
					AttributeName: 'uid',
					KeyType: 'RANGE'
				}
			],
			AttributeDefinitions: [
				{
					AttributeType: 'S',
					AttributeName: 'gid'
				},
				{
					AttributeType: 'S',
					AttributeName: 'uid'
				}
			],
			ProvisionedThroughput: {
				ReadCapacityUnits: self.read,
				WriteCapacityUnits: self.write
			}
		};

		try {

			for (const table of data.tables) {
				options.TableName = table;
				await self.dynamo.createTable(options).promise();
			}

		} catch (error) {
			if (error.code !== 'ResourceInUseException') {
				throw error;
			}
		}

	}

	async remove (data) {
		const self = this;

		data = data || {};

		if (!data.gid) throw new Error('gid required');
		if (!data.uid) throw new Error('uid required');
		if (!data.table) throw new Error('table required');

		const options = {
			TableName: data.table,
			Key: {
				gid: { S: data.gid },
				uid: { S: data.uid }
			}
		};

		return await self.dynamo.deleteItem(options).promise();
	}

	async get (data) {
		const self = this;

		data = data || {};

		if (!data.gid) throw new Error('gid required');
		if (!data.uid) throw new Error('uid required');
		if (!data.table) throw new Error('table required');

		const options = {
			TableName: data.table,
			Key: {
				gid: { S: data.gid },
				uid: { S: data.uid }
			}
		};

		const { Item } = await self.dynamo.getItem(options).promise();

		return self.unmarshall(Item);
	}

	// Creates a new item or replaces an old item with a new item.
	async put (data) {
		const self = this;

		data = data || {};

		if (!data.gid) throw new Error('gid required');
		if (!data.uid) throw new Error('uid required');
		if (!data.table) throw new Error('table required');

		const options = {
			TableName: data.table,
			Item: {
				gid: { S: data.gid },
				uid: { S: data.uid }
			}
		};

		if (data.body) {
			Object.assign(options.Item, self.marshall(data.body));
		}

		return await self.dynamo.putItem(options).promise();
	}

	// Edits an existing item or adds a new item if it does not exist.
	async update (data) {
		const self = this;

		data = data || {};

		if (!data.gid) throw new Error('gid required');
		if (!data.uid) throw new Error('uid required');
		if (!data.table) throw new Error('table required');

		const options = {
			TableName: data.table,
			Key: {
				gid: { S: data.gid },
				uid: { S: data.uid }
			},
			UpdateExpression: '',
			ExpressionAttributeNames: {},
			ExpressionAttributeValues: {}
		};

		if (data.body) {
			for (const name in data.body) {
				options.ExpressionAttributeNames[`#${name}`] = name;
				options.ExpressionAttributeValues[`:${name}`] = self.input(data.body[name]);
				options.UpdateExpression += `${options.UpdateExpression ? ',' : 'set'} #${name} = :${name}`;
			}
		}

		return await self.dynamo.updateItem(options).promise();
	}

	async query (data) {
		const self = this;

		data = data || {};

		if (!data.gid) throw new Error('gid required');
		if (!data.table) throw new Error('table required');

		const options = {
			TableName: data.table,
			FilterExpression: '',
			KeyConditionExpression: '',
			ExpressionAttributeNames: {},
			ExpressionAttributeValues: {}
		};

		options.KeyConditionExpression = '#gid = :gid';
		options.ExpressionAttributeNames['#gid'] = 'gid';
		options.ExpressionAttributeValues[':gid'] = { S: data.gid };

		if (data.uid) {
			options.KeyConditionExpression += ', #uid = :uid';
			options.ExpressionAttributeNames['#uid'] = 'uid';
			options.ExpressionAttributeValues[':uid'] = { S: data.uid };
		}

		if (data.body) {
			for (const name in data.body) {
				options.ExpressionAttributeNames[`#${name}`] = name;
				options.ExpressionAttributeValues[`:${name}`] = self.input(data.body[name]);
				options.FilterExpression += `${options.FilterExpression ? ',' : ''} #${name} = :${name}`;
			}
		}

		const { Items } = await self.dynamo.query(options).promise();

		return Items.map(function (item) {
			return self.unmarshall(item);
		});
	}
}
