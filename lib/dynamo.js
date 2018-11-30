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
			apiVersion: data.version,
			credentials: data.credentials
		};

		self.dynamo = new Aws.DynamoDB(options);
	}

	async setup (tables) {
		const self = this;

		if (!tables) throw new Error('tables array required');
		if (!tables.length) throw new Error('tables names required');

		const promises = [];

		for (let i = 0; i < tables.length; i++) {
			const table = typeof tables[i] === 'string' ? { name: tables[i] } : tables[i];

			const options = {
				TableName: table.name,
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

			if (table.index) {
				options.GlobalSecondaryIndexes = options.GlobalSecondaryIndexes || [];
				options.GlobalSecondaryIndexes.push({
					IndexName: 'uid',
					KeySchema: [{
						KeyType: 'HASH',
						AttributeName: 'uid',
					}],
					Projection: {
						ProjectionType: 'ALL'
					},
					ProvisionedThroughput: {
						ReadCapacityUnits: self.read,
						WriteCapacityUnits: self.write
					}
				});
			}

			const promise = Promise.resolve().then(function () {
				return self.dynamo.createTable(options).promise();
			}).then(function () {
				return self.dynamo.waitFor('tableExists', { TableName: options.TableName }).promise();
			}).catch(function (error) {
				if (error.code !== 'ResourceInUseException') {
					throw error;
				}
			});

			promises.push(promise);
		}

		await Promise.all(promises);
	}

	async remove (table, data) {
		const self = this;

		data = data || {};

		if (!table) throw new Error('table required');
		if (!data.gid) throw new Error('gid required');
		if (!data.uid) throw new Error('uid required');

		const options = {
			TableName: table,
			Key: {
				gid: { S: data.gid },
				uid: { S: data.uid }
			}
		};

		return await self.dynamo.deleteItem(options).promise();
	}

	async get (table, data) {
		const self = this;

		data = data || {};

		if (!table) throw new Error('table required');
		if (!data.gid) throw new Error('gid required');
		if (!data.uid) throw new Error('uid required');

		const options = {
			TableName: table,
			Key: {
				gid: { S: data.gid },
				uid: { S: data.uid }
			}
		};

		const { Item } = await self.dynamo.getItem(options).promise();

		return Item ? self.unmarshall(Item) : null;
	}

	// Creates a new item or replaces an old item with a new item.
	async put (table, data) {
		const self = this;

		data = data || {};

		if (!table) throw new Error('table required');
		if (!data.gid) throw new Error('gid required');
		if (!data.uid) throw new Error('uid required');

		const options = {
			TableName: table,
			Item: self.marshall(data)
		};

		return await self.dynamo.putItem(options).promise();
	}

	// Edits an existing item or adds a new item if it does not exist.
	async update (table, data) {
		const self = this;

		data = data || {};

		if (!table) throw new Error('table required');
		if (!data.gid) throw new Error('gid required');
		if (!data.uid) throw new Error('uid required');

		const options = {
			TableName: table,
			Key: {
				gid: { S: data.gid },
				uid: { S: data.uid }
			},
			UpdateExpression: '',
			ExpressionAttributeNames: {},
			ExpressionAttributeValues: {}
		};

		for (const name in data) {
			if (name === 'gid' || name === 'uid') continue;
			options.ExpressionAttributeNames[`#${name}`] = name;
			options.ExpressionAttributeValues[`:${name}`] = self.input(data[name]);
			options.UpdateExpression += `${options.UpdateExpression ? ',' : 'set'} #${name} = :${name}`;
		}

		return await self.dynamo.updateItem(options).promise();
	}

	async query (table, data) {
		const self = this;

		data = data || {};

		if (!table) throw new Error('table required');

		const options = {
			TableName: table,
			KeyConditionExpression: '',
			ExpressionAttributeNames: {},
			ExpressionAttributeValues: {}
		};

		if (data.gid) {
			options.KeyConditionExpression += '#gid = :gid';
			options.ExpressionAttributeNames['#gid'] = 'gid';
			options.ExpressionAttributeValues[':gid'] = self.input(data.gid);
		}

		if (data.gid && data.uid) {
			options.KeyConditionExpression += ', ';
		}

		if (!data.gid && data.uid) {
			options.IndexName = 'uid';
		}

		if (data.uid) {
			options.KeyConditionExpression += '#uid = :uid';
			options.ExpressionAttributeNames['#uid'] = 'uid';
			options.ExpressionAttributeValues[':uid'] = self.input(data.uid);
		}

		for (const name in data) {
			if (name === 'gid' || name === 'uid') continue;
			if (!options.FilterExpression) options.FilterExpression = '';
			options.ExpressionAttributeNames[`#${name}`] = name;
			options.ExpressionAttributeValues[`:${name}`] = self.input(data[name]);
			options.FilterExpression += `${options.FilterExpression ? ',' : ''} #${name} = :${name}`;
		}

		const { Items } = await self.dynamo.query(options).promise();

		return Items.map(function (item) {
			return item ? self.unmarshall(item) : null;
		});
	}
}
