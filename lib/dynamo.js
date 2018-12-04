'use stric';

const Aws = require('aws-sdk');

module.exports = class Dynamo {

	constructor (data) {
		const self = this;

		data = data || {};

		self.invoked = false;
		self.tables = new Map();
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
		const promises = [];

		if (self.invoked) {
			return console.warn('Sedb.setup - previously invoked');
		} else {
			self.invoked = true;
		}

		if (!tables) throw new Error('Sedb.setup - tables required');

		tables = tables.constructor === Array ? tables : [ tables ];

		for (let table of tables) {
			table = typeof table === 'string' ? { name: table } : table;

			const options = {
				KeySchema: [],
				TableName: table.name,
				AttributeDefinitions: [],
				ProvisionedThroughput: {
					ReadCapacityUnits: self.read,
					WriteCapacityUnits: self.write
				}
			};

			if (!table.schema) {
				table.schema = [
					{
						hash: 'uid'
					}
				];
			}

			const schemas = table.schema.constructor === Array ? table.schema : [ table.schema ];

			self.tables.set(table.name, new Map());

			for (const schema of schemas) {

				if (!schema.hash && !schema.range) throw new Error('Sedb.setup - table schema hash or range required');
				if (!schema.hash && schema.range) throw new Error('Sedb.setup - table schema range requires a hash');

				if (schema.gsi) {
					options.GlobalSecondaryIndexes = options.GlobalSecondaryIndexes || [];

					options.GlobalSecondaryIndexes.push({
						KeySchema: [],
						IndexName: schema.hash,
						Projection: {
							ProjectionType: 'ALL'
						},
						ProvisionedThroughput: {
							ReadCapacityUnits: self.read,
							WriteCapacityUnits: self.write
						}
					});

				}

				if (schema.hash) {
					options.AttributeDefinitions.push({
						AttributeType: 'S',
						AttributeName: schema.hash
					});

					if (schema.gsi) {
						options.GlobalSecondaryIndexes[options.GlobalSecondaryIndexes.length-1].KeySchema.push({
							KeyType: 'HASH',
							AttributeName: schema.hash
						});
					} else {
						options.KeySchema.push({
							KeyType: 'HASH',
							AttributeName: schema.hash
						});
					}

					self.tables.get(table.name).set(schema.hash, schema);
				}

				if (schema.range) {
					options.AttributeDefinitions.push({
						AttributeType: 'S',
						AttributeName: schema.range
					});

					if (schema.gsi) {
						options.GlobalSecondaryIndexes[options.GlobalSecondaryIndexes.length-1].KeySchema.push({
							KeyType: 'RANGE',
							AttributeName: schema.range
						});
					} else {
						options.KeySchema.push({
							KeyType: 'RANGE',
							AttributeName: schema.range
						});
					}

					self.tables.get(table.name).set(schema.range, schema);
				}

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

		if (!table) throw new Error('Sedb.remove - table name required');

		const options = {
			TableName: table,
			Key: self.marshall(data)
		};

		return self.dynamo.deleteItem(options).promise();
	}

	async get (table, data) {
		const self = this;

		data = data || {};

		if (!table) throw new Error('Sedb.get - table name required');

		const options = {
			TableName: table,
			Key: self.marshall(data)
		};

		const { Item } = await self.dynamo.getItem(options).promise();

		return Item ? self.unmarshall(Item) : null;
	}

	// Creates a new item or replaces an old item with a new item.
	async put (table, data) {
		const self = this;

		data = data || {};

		if (!table) throw new Error('Sedb.put - table name required');

		const options = {
			TableName: table,
			Item: self.marshall(data)
		};

		return self.dynamo.putItem(options).promise();
	}

	// Edits an existing item or adds a new item if it does not exist.
	async update (table, data) {
		const self = this;

		data = data || {};

		if (!table) throw new Error('Sedb.update - table name required');

		const options = {
			Key: {},
			TableName: table,
			UpdateExpression: '',
			ExpressionAttributeNames: {},
			ExpressionAttributeValues: {}
		};

		for (const name in data) {

			if (self.tables.get(table).has(name)) {
				options.Key[name] = self.input(data[name]);
				continue;
			}

			options.ExpressionAttributeNames[`#${name}`] = name;
			options.ExpressionAttributeValues[`:${name}`] = self.input(data[name]);
			options.UpdateExpression += `${options.UpdateExpression ? ',' : 'set'} #${name} = :${name}`;
		}

		return self.dynamo.updateItem(options).promise();
	}

	async query (table, data) {
		const self = this;

		data = data || {};

		if (!table) throw new Error('Sedb.query - table name required');

		const options = {
			TableName: table,
			KeyConditionExpression: '',
			ExpressionAttributeNames: {},
			ExpressionAttributeValues: {}
		};

		let gsi = null;
		let primary = false;

		for (const name in data) {
			const schema = self.tables.get(table).get(name);

			if (schema) {

				if (schema.gsi && schema.hash === name) {
					gsi = schema;
				} else {
					primary = true;
				}

				options.KeyConditionExpression += `${options.KeyConditionExpression ? ' AND ' : ''}#${name} = :${name}`;
			} else {

				if (!options.FilterExpression) {
					options.FilterExpression = '';
				}

				options.FilterExpression += `${options.FilterExpression ? ',' : ''}#${name} = :${name}`;
			}

			options.ExpressionAttributeNames[`#${name}`] = name;
			options.ExpressionAttributeValues[`:${name}`] = self.input(data[name]);
		}

		if (!primary) {
			options.IndexName = gsi.hash;
		}

		const { Items } = await self.dynamo.query(options).promise();

		return Items.map(function (item) {
			return item ? self.unmarshall(item) : null;
		});
	}

}
