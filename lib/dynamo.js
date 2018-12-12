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
						IndexName: schema.gsi,
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

	async get (table, condition) {
		const self = this;

		if (!table) throw new Error('Sedb.get - table name required');
		if (!condition) throw new Error('Sedb.get - condition required');

		const options = {
			TableName: table,
			Key: self.marshall(condition)
		};

		const { Item } = await self.dynamo.getItem(options).promise();

		return Item ? self.unmarshall(Item) : null;
	}

	async remove (table, condition) {
		const self = this;

		if (!table) throw new Error('Sedb.remove - table name required');
		if (!condition) throw new Error('Sedb.remove - condition required');

		const options = {
			TableName: table,
			Key: self.marshall(condition)
		};

		await self.dynamo.deleteItem(options).promise();
	}

	// creates a new item or replaces an old item with a new item
	async put (table, data) {
		const self = this;

		if (!data) throw new Error('Sedb.put - data required');
		if (!table) throw new Error('Sedb.put - table name required');

		const options = {
			TableName: table,
			Item: self.marshall(data),
		};

		await self.dynamo.putItem(options).promise();
	}

	// todo: add a item if it does not exist
	async add (table, data, options) {
		const self = this;

		if (!data) throw new Error('Sedb.add - data required');
		if (!table) throw new Error('Sedb.add - table name required');

		const payload = {
			TableName: table,
			ConditionExpression: '',
			Item: self.marshall(data),
			ExpressionAttributeNames: {}
			// ExpressionAttributeValues: {}
		};

		// verify item does not exists
		for (const name in data) {
			const schema = self.tables.get(table).get(name);
			if (schema && !schema.gsi) {
				payload.ExpressionAttributeNames[`#${name}`] = name;
				payload.ConditionExpression += `${payload.ConditionExpression ? ' AND ' : ''}attribute_not_exists(#${name})`;
			}
		}

		if (options && options.conditions) {
			payload.ExpressionAttributeValues = {};
			payload.ConditionExpression += ' AND ';
			for (const condition of options.conditions) {
				payload.ConditionExpression += condition.data;
				payload.ExpressionAttributeNames[`#${condition.name}`] = condition.name;
				payload.ExpressionAttributeValues[`:${condition.name}`] = self.input(condition.value);
			}
		}

		try {
			await self.dynamo.putItem(payload).promise();
		} catch (error) {
			if (error.code === 'ConditionalCheckFailedException') {
				throw new Error('Sedb.add - item exists');
			} else {
				throw error;
			}
		}

	}

	// updates a item if the condition is true
	async update (table, data, options) {
		const self = this;

		if (!data) throw new Error('Sedb.update - data required');
		if (!table) throw new Error('Sedb.update - table name required');

		const payload = {
			Key: {},
			TableName: table,
			UpdateExpression: '',
			ConditionExpression: '',
			ExpressionAttributeNames: {},
			ExpressionAttributeValues: {}
		};

		for (const name in data) {
			const schema = self.tables.get(table).get(name);

			// verify item exists
			if (schema && !schema.gsi) {
				payload.Key[name] = self.input(data[name]);
				payload.ExpressionAttributeNames[`#${name}`] = name;
				payload.ConditionExpression += `${payload.ConditionExpression ? ' AND ' : ''}attribute_exists(#${name})`;
				continue;
			}

			payload.ExpressionAttributeNames[`#${name}`] = name;
			payload.ExpressionAttributeValues[`:${name}`] = self.input(data[name]);
			payload.UpdateExpression += `${payload.UpdateExpression ? ',' : 'set'} #${name} = :${name}`;
		}

		if (options && options.conditions) {
			payload.ConditionExpression = `(${payload.ConditionExpression})`;
<<<<<<< HEAD
			if (options.conditions.constructor === Array) {
				for (const condition of options.conditions) {
					if (typeof condition === 'string') {
						payload.ConditionExpression += ` ${condition}`;
					} else {
						payload.ConditionExpression += ` AND `;
						payload.ConditionExpression += `#${condition.name}=:${condition.name}`;
						payload.ExpressionAttributeNames[`#${condition.name}`] = condition.name;
						payload.ExpressionAttributeValues[`:${condition.name}`] = self.input(condition.value);
					}
				}
			} else if (options.conditions.constructor === Object) {
				for (const name in options.conditions) {
					payload.ConditionExpression += ` AND `;
					payload.ConditionExpression += `#${name}=:${name}`;
					payload.ExpressionAttributeNames[`#${name}`] = name;
					payload.ExpressionAttributeValues[`:${name}`] = self.input(options.conditions[name]);
=======
			for (const condition of options.conditions) {
				if (typeof condition === 'string') {
					payload.ConditionExpression += ` ${condition}`;
				} else {
					payload.ConditionExpression += ` AND `;
					payload.ConditionExpression += `#${condition.name}=:${condition.name}`;
					payload.ExpressionAttributeNames[`#${condition.name}`] = condition.name;
					payload.ExpressionAttributeValues[`:${condition.name}`] = self.input(condition.value);
>>>>>>> 506762e841ce2c314754894558695b8fed12584c
				}
			}
		}

		// Dynamo.updateItem: Edits an existing item or adds a new item if it does not exist
		await self.dynamo.updateItem(payload).promise();
	}

	async query (table, condition) {
		const self = this;

		if (!table) throw new Error('Sedb.query - table name required');
		if (!condition) throw new Error('Sedb.query - condition required');

		const options = {
			TableName: table,
			KeyConditionExpression: '',
			ExpressionAttributeNames: {},
			ExpressionAttributeValues: {}
		};

		for (const name in condition) {
			const schema = self.tables.get(table).get(name);

			if (schema) {

				if (schema.gsi && schema.hash === name) {
					options.IndexName = schema.gsi;
				}

				options.KeyConditionExpression += `${options.KeyConditionExpression ? ' AND ' : ''}#${name} = :${name}`;
			} else {

				if (!options.FilterExpression) {
					options.FilterExpression = '';
				}

				options.FilterExpression += `${options.FilterExpression ? ' AND ' : ''}#${name} = :${name}`;
			}

			options.ExpressionAttributeNames[`#${name}`] = name;
			options.ExpressionAttributeValues[`:${name}`] = self.input(condition[name]);
		}

		const { Items } = await self.dynamo.query(options).promise();

		return Items.map(function (item) {
			return item ? self.unmarshall(item) : null;
		});
	}

}
