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

	table (name) {
		const self = this;

		return Object.defineProperties({}, {
			setup: {
				enumerable: true,
				value: self.setup
			},
			get: {
				enumerable: true,
				value: self.get.bind(self, name)
			},
			put: {
				enumerable: true,
				value: self.put.bind(self, name)
			},
			add: {
				enumerable: true,
				value: self.add.bind(self, name)
			},
			scan: {
				enumerable: true,
				value: self.scan.bind(self, name)
			},
			query: {
				enumerable: true,
				value: self.query.bind(self, name)
			},
			update: {
				enumerable: true,
				value: self.update.bind(self, name)
			},
			remove: {
				enumerable: true,
				value: self.remove.bind(self, name)
			}
		});
	}

	async mergeConditon (data, condition) {
		const self = this;

		if (typeof condition !== 'object') {
			throw new Error ('Sedb.mergeConditon - invalid condition type');
		}

		data = Object.assign({}, data);
		data.ExpressionAttributeNames = data.ExpressionAttributeNames || {};
		data.ExpressionAttributeValues = data.ExpressionAttributeValues || {};

		if (data.ConditionExpression) {
			data.ConditionExpression = `(${data.ConditionExpression})`;
		} else {
			data.ConditionExpression = '';
		}

		if (condition.constructor === Array) {

			for (const item of condition) {

				if (typeof item === 'string') {
					data.ConditionExpression += ` ${item}`;
				} else {
					data.ConditionExpression +=  data.ConditionExpression ? ' AND ' : '';
					data.ConditionExpression += `#${item.name}=:${item.name}`;
					data.ExpressionAttributeNames[`#${item.name}`] = item.name;
					data.ExpressionAttributeValues[`:${item.name}`] = self.input(item.value);
				}

			}

			return data;
		}

		if (condition.constructor === Object) {

			for (const name in condition) {
				data.ConditionExpression +=  data.ConditionExpression ? ' AND ' : '';
				data.ConditionExpression += `#${name}=:${name}`;
				data.ExpressionAttributeNames[`#${name}`] = name;
				data.ExpressionAttributeValues[`:${name}`] = self.input(condition[name]);
			}

			return data;
		}

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

	// creates a new item or replaces an old item with a new item
	async put (table, item) {
		const self = this;

		if (!item) throw new Error('Sedb.put - item required');
		if (!table) throw new Error('Sedb.put - table name required');

		let options = {
			TableName: table,
			Item: self.marshall(item),
		};

		await self.dynamo.putItem(options).promise();
	}

	// adds a item if it does not exist and the condition is true
	async add (table, item, option) {
		const self = this;

		if (!item) throw new Error('Sedb.add - item required');
		if (!table) throw new Error('Sedb.add - table name required');

		let data = {
			TableName: table,
			ConditionExpression: '',
			Item: self.marshall(item),
			ExpressionAttributeNames: {}
		};

		// verify item does not exists
		for (const name in item) {
			const schema = self.tables.get(table).get(name);

			if (schema && !schema.gsi) {
				data.ExpressionAttributeNames[`#${name}`] = name;
				data.ConditionExpression += `${data.ConditionExpression ? ' AND ' : ''}attribute_not_exists(#${name})`;
			}

		}

		if (typeof option === 'object' && typeof option.condition === 'object') {
			data = await self.mergeConditon(data, option.condition);
		}

		await self.dynamo.putItem(data).promise();
	}

	async remove (table, item, option) {
		const self = this;

		if (!item) throw new Error('Sedb.remove - item required');
		if (!table) throw new Error('Sedb.remove - table name required');

		let data = {
			TableName: table,
			Key: self.marshall(item)
		};

		if (typeof option === 'object' && typeof option.condition === 'object') {
			data = await self.mergeConditon(data, option.condition);
		}

		await self.dynamo.deleteItem(data).promise();
	}

	// updates a item if it exist and the condition is true
	async update (table, item, option) {
		const self = this;

		if (!item) throw new Error('Sedb.update - item required');
		if (!table) throw new Error('Sedb.update - table name required');

		let data = {
			Key: {},
			TableName: table,
			UpdateExpression: '',
			ConditionExpression: '',
			ExpressionAttributeNames: {},
			ExpressionAttributeValues: {}
		};

		for (const name in item) {
			const schema = self.tables.get(table).get(name);

			// verify does item exists
			if (schema && !schema.gsi) {
				data.Key[name] = self.input(item[name]);
				data.ExpressionAttributeNames[`#${name}`] = name;
				data.ConditionExpression += `${data.ConditionExpression ? ' AND ' : ''}attribute_exists(#${name})`;
				continue;
			}

			data.ExpressionAttributeNames[`#${name}`] = name;
			data.ExpressionAttributeValues[`:${name}`] = self.input(item[name]);
			data.UpdateExpression += `${data.UpdateExpression ? ',' : 'set'} #${name} = :${name}`;
		}

		if (typeof option === 'object' && typeof option.condition === 'object') {
			data = await self.mergeConditon(data, option.condition);
		}

		// Dynamo.updateItem: Edits an existing item or adds a new item if it does not exist
		await self.dynamo.updateItem(data).promise();
	}

	async query (table, condition) {
		const self = this;

		if (!table) throw new Error('Sedb.query - table name required');
		if (!condition) throw new Error('Sedb.query - condition required');

		let data = {
			TableName: table,
			KeyConditionExpression: '',
			ExpressionAttributeNames: {},
			ExpressionAttributeValues: {}
		};

		for (const name in condition) {
			const schema = self.tables.get(table).get(name);

			if (schema) {

				if (schema.gsi && schema.hash === name) {
					data.IndexName = schema.gsi;
				}

				data.KeyConditionExpression += `${data.KeyConditionExpression ? ' AND ' : ''}#${name} = :${name}`;
			} else {

				if (!data.FilterExpression) {
					data.FilterExpression = '';
				}

				data.FilterExpression += `${data.FilterExpression ? ' AND ' : ''}#${name} = :${name}`;
			}

			data.ExpressionAttributeNames[`#${name}`] = name;
			data.ExpressionAttributeValues[`:${name}`] = self.input(condition[name]);
		}

		const { Items } = await self.dynamo.query(data).promise();

		return Items.map(function (item) {
			return item ? self.unmarshall(item) : null;
		});
	}

	async scan (table, condition) {
		const self = this;

		if (!table) throw new Error('Sedb.scan - table name required');
		if (!condition) throw new Error('Sedb.scan - condition required');

		let data = {
			TableName: table,
			ExpressionAttributeNames: {},
			ExpressionAttributeValues: {}
		};

		for (const name in condition) {

			if (schema) {

				if (schema.gsi) {
					data.IndexName = schema.gsi;
				}

			} else {

				if (!data.FilterExpression) {
					data.FilterExpression = '';
				}

				data.FilterExpression += `${data.FilterExpression ? ' AND ' : ''}#${name} = :${name}`;
			}

			data.ExpressionAttributeNames[`#${name}`] = name;
			data.ExpressionAttributeValues[`:${name}`] = self.input(condition[name]);
		}

		const { Items } = await self.dynamo.scan(data).promise();

		return Items.map(function (item) {
			return item ? self.unmarshall(item) : null;
		});
	}

}
