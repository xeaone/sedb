'use stric';

const Aws = require('aws-sdk');

module.exports = class Dynamo {

	constructor (options = {}) {

		this.schema = new Map();
        this.name = options.name;
		this.read = options.read || 1,
		this.write = options.write || 1;

		this.input = Aws.DynamoDB.Converter.input;
		this.output = Aws.DynamoDB.Converter.output;

		this.marshall = Aws.DynamoDB.Converter.marshall;
		this.unmarshall = Aws.DynamoDB.Converter.unmarshall;

        delete options.read;
        delete options.write;
        delete options.name;

        if (options.version) options.apiVersion = options.version;
        if (typeof this.name !== 'string') throw new Error('name required');

		this.dynamo = new Aws.DynamoDB(options);
	}

	async mergeConditon (data, condition) {

		if (typeof data !== 'object') throw new Error ('data object required');
		if (typeof condition !== 'object') throw new Error ('condition object or array required');

		data.ExpressionAttributeNames = data.ExpressionAttributeNames || {};
		data.ExpressionAttributeValues = data.ExpressionAttributeValues || {};

		if (data.ConditionExpression) {
			data.ConditionExpression = `(${data.ConditionExpression})`;
		} else {
			data.ConditionExpression = '';
		}

		if (condition instanceof Array) {
			for (const item of condition) {
				if (typeof item === 'string') {
					data.ConditionExpression += ` ${item}`;
				} else {
					data.ConditionExpression += data.ConditionExpression ? ' AND ' : '';
					data.ConditionExpression += `#${item.name}=:${item.name}`;
					data.ExpressionAttributeNames[`#${item.name}`] = item.name;
					data.ExpressionAttributeValues[`:${item.name}`] = this.input(item.value);
				}
			}
		} else {
			for (const name in condition) {
				data.ConditionExpression += data.ConditionExpression ? ' AND ' : '';
				data.ConditionExpression += `#${name}=:${name}`;
				data.ExpressionAttributeNames[`#${name}`] = name;
				data.ExpressionAttributeValues[`:${name}`] = this.input(condition[name]);
			}
		}

	}

	async setup (schema) {
		if (!schema) throw new Error('schema required');

		const data = {
			KeySchema: [],
			TableName: this.name,
			AttributeDefinitions: [],
			ProvisionedThroughput: {
				ReadCapacityUnits: this.read,
				WriteCapacityUnits: this.write
			}
		};

		const schemas = schema instanceof Array ? schema : [ schema ];
		for (const { hash, range, gsi } of schemas) {

			if (!hash && !range) throw new Error('schema hash or range required');
			if (!hash && range) throw new Error('schema range requires a hash');

			if (gsi) {
				data.GlobalSecondaryIndexes = data.GlobalSecondaryIndexes || [];

				data.GlobalSecondaryIndexes.push({
					KeySchema: [],
					IndexName: gsi,
					Projection: {
						ProjectionType: 'ALL'
					},
					ProvisionedThroughput: {
						ReadCapacityUnits: this.read,
						WriteCapacityUnits: this.write
					}
				});

			}

			if (hash) {
				data.AttributeDefinitions.push({
					AttributeType: 'S',
					AttributeName: hash
				});

				if (gsi) {
					data.GlobalSecondaryIndexes[data.GlobalSecondaryIndexes.length-1].KeySchema.push({
						KeyType: 'HASH',
						AttributeName: hash
					});
				} else {
					data.KeySchema.push({
						KeyType: 'HASH',
						AttributeName: hash
					});
				}

				this.schema.set(hash, { hash, range, gsi });
			}

			if (range) {
				data.AttributeDefinitions.push({
					AttributeType: 'S',
					AttributeName: range
				});

				if (gsi) {
					data.GlobalSecondaryIndexes[data.GlobalSecondaryIndexes.length-1].KeySchema.push({
						KeyType: 'RANGE',
						AttributeName: range
					});
				} else {
					data.KeySchema.push({
						KeyType: 'RANGE',
						AttributeName: range
					});
				}

				this.schema.set(range, { hash, range, gsi });
			}

		}

        try {
            await this.dynamo.createTable(data).promise();
            await this.dynamo.waitFor('tableExists', { TableName: data.TableName }).promise();
        } catch (error) {
            if (error.code !== 'ResourceInUseException') throw error;
        }

	}

	// adds a item if it does not exist and the condition is true
	async add (item, options) {
		if (!item) throw new Error('item required');

        const Item = {};
        const TableName = this.name;
        let ConditionExpression = '';
	    const ExpressionAttributeNames = {};

		// verify item does not exists
		for (const name in item) {
            const value = item[name];
            if (value === undefined) continue;
			const schema = this.schema.get(name);

			if (schema && !schema.gsi) {
				ExpressionAttributeNames[`#${name}`] = name;
				ConditionExpression += `${ConditionExpression ? ' AND ' : ''}attribute_not_exists(#${name})`;
			}

			Item[name] = this.input(value);
		}

        const data = { Item, TableName, ConditionExpression, ExpressionAttributeNames };

		if (typeof options === 'object' && typeof options.condition === 'object') {
			await this.mergeConditon(data, options.condition);
		}

		await this.dynamo.putItem(data).promise();
	}

	// updates a item if it exist and the condition is true
	async update (item, options) {
		if (!item) throw new Error('item required');

        const Key = {};
		const TableName = this.name;
		let UpdateExpression = '';
		let ConditionExpression = '';
		const ExpressionAttributeNames = {};
		const ExpressionAttributeValues = {};

		for (const name in item) {
            const value = item[name];
            if (value === undefined) continue;
			const schema = this.schema.get(name);

			// verify item exists
			if (schema && !schema.gsi) {
				Key[name] = this.input(value);
				ExpressionAttributeNames[`#${name}`] = name;
				ConditionExpression += `${ConditionExpression ? ' AND ' : ''}attribute_exists(#${name})`;
				continue;
			}

			ExpressionAttributeNames[`#${name}`] = name;
			ExpressionAttributeValues[`:${name}`] = this.input(value);
			UpdateExpression += `${UpdateExpression ? ',' : 'set'} #${name} = :${name}`;
		}

		const data = {
			Key,
			TableName,
			UpdateExpression,
			ConditionExpression,
			ExpressionAttributeNames,
			ExpressionAttributeValues
		};

		if (typeof options === 'object' && typeof options.condition === 'object') {
			await this.mergeConditon(data, options.condition);
		}

		// Dynamo.updateItem: Edits an existing item or adds a new item if it does not exist
		await this.dynamo.updateItem(data).promise();
	}

	// replaces existing item or creates new item.
	async put (item) {
		if (!item) throw new Error('item required');

        const Item = {};
        const TableName = this.name;

        for (const name in item) {
            const value = item[name];
            if (value === undefined) continue;
    		Item[name] = this.input(value);
        }

		const data = { Item, TableName };
		await this.dynamo.putItem(data).promise();
	}

    // removes item if it exists
	async remove (item, options) {
		if (!item) throw new Error('item required');

        const Key = {};
        const TableName = this.table;

        for (const name in item) {
            const value = item[name];
            if (value === undefined) continue;
    		Key[name] = this.input(value);
        }

		const data = { TableName, Key };

		if (typeof options === 'object' && typeof options.condition === 'object') {
			await this.mergeConditon(data, options.condition);
		}

		const { Item } = await this.dynamo.deleteItem(data).promise();
		return Item ? this.unmarshall(Item) : null;
	}

	async get (condition) {
		if (!condition) throw new Error('condition required');

        const Key = {};
        const TableName = this.name;

        for (const name in condition) {
            const value = condition[name];
            if (value === undefined) continue;
    		Key[name] = this.input(value);
        }

		const data = { TableName, Key };
		const { Item } = await this.dynamo.getItem(data).promise();
		return Item ? this.unmarshall(Item) : null;
	}

	async query (condition) {
		if (!condition) throw new Error('condition required');

		const data = {
			TableName: this.name,
			KeyConditionExpression: '',
			ExpressionAttributeNames: {},
			ExpressionAttributeValues: {}
		};

		for (const name in condition) {
            const value = condition[name];
            if (value === undefined) continue
			const schema = this.schema.get(name);

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
			data.ExpressionAttributeValues[`:${name}`] = this.input(condition[name]);
		}

		const { Items } = await this.dynamo.query(data).promise();
		return Items.map(item => item ? this.unmarshall(item) : null);
	}

	async scan (condition) {
		if (!condition) throw new Error('condition required');

		const data = {
			TableName: this.name,
			ExpressionAttributeNames: {},
			ExpressionAttributeValues: {}
		};

		for (const name in condition) {
            const value = condition[name];
            if (value === undefined) continue;
			const schema = this.schema.get(name);

			if (schema) {

				if (schema.gsi && schema.hash === name) {
					data.IndexName = schema.gsi;
				}

				// if (schema.gsi) {
				// 	data.IndexName = schema.gsi;
				// }

			} else {

				if (!data.FilterExpression) {
					data.FilterExpression = '';
				}

				data.FilterExpression += `${data.FilterExpression ? ' AND ' : ''}#${name} = :${name}`;
			}

			data.ExpressionAttributeNames[`#${name}`] = name;
			data.ExpressionAttributeValues[`:${name}`] = this.input(condition[name]);
		}

		const { Items } = await this.dynamo.scan(data).promise();
		return Items.map(item => item ? this.unmarshall(item) : null);
	}

}
