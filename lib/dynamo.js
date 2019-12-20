const Aws = require('aws-sdk');
const Version = require('./version.js');
const Credential = require('./credential.js');

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

        delete options.name;
        delete options.read;
        delete options.write;

        if (typeof this.name !== 'string') throw new Error('name required');

        Version(options);
        Credential(options);

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
                const KeyType = 'HASH';
                const AttributeName = hash;

                if (!data.AttributeDefinitions.find(def => def.AttributeName === hash)) {
                    data.AttributeDefinitions.push({ AttributeType: 'S', AttributeName });
                }

                if (gsi) {
                    const last = data.GlobalSecondaryIndexes.length-1;
                    data.GlobalSecondaryIndexes[last].KeySchema.push({ KeyType, AttributeName });
                } else {
                    data.KeySchema.push({ KeyType, AttributeName });
                }

                this.schema.set(hash, { hash, range, gsi });
            }

            if (range) {
                const KeyType = 'RANGE';
                const AttributeName = range;

                if (!data.AttributeDefinitions.find(def => def.AttributeName === range)) {
                    data.AttributeDefinitions.push({ AttributeType: 'S', AttributeName });
                }

                if (gsi) {
                    const last = data.GlobalSecondaryIndexes.length-1;
                    data.GlobalSecondaryIndexes[last].KeySchema.push({ KeyType, AttributeName });
                } else {
                    data.KeySchema.push({ KeyType, AttributeName });
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
        const ExpressionAttributeNames = {};

        let ConditionExpression = '';

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
    // undefined values will remove attributes
    async update (item, options) {
        if (!item) throw new Error('item required');

        const Key = {};
        const TableName = this.name;
        const ExpressionAttributeNames = {};
        const ExpressionAttributeValues = {};

        let UpdateExpression = '';
        let ConditionExpression = '';

        for (const name in item) {
            const value = item[name];

            const schema = this.schema.get(name);

            // verify item exists
            if (schema && !schema.gsi) {
                Key[name] = this.input(value);
                ExpressionAttributeNames[`#${name}`] = name;
                ConditionExpression += `${ConditionExpression ? ' AND ' : ''}attribute_exists(#${name})`;
                continue;
            }

            ExpressionAttributeNames[`#${name}`] = name;

            if (value === undefined) {
                UpdateExpression += `${UpdateExpression ? ',' : 'REMOVE'} #${name}`;
            } else {
                ExpressionAttributeValues[`:${name}`] = this.input(value);
                UpdateExpression += `${UpdateExpression ? ',' : 'SET'} #${name} = :${name}`;
            }

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
        const TableName = this.name;

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

        const TableName = this.name;
        const ExpressionAttributeNames = {};
        const ExpressionAttributeValues = {};

        let IndexName;
        let FilterExpression = '';
        let KeyConditionExpression = '';

        for (const name in condition) {
            const value = condition[name];
            if (value === undefined) continue;

            const schema = this.schema.get(name);

            if (schema) {
                if (schema.gsi && schema.hash === name) IndexName = schema.gsi;
                KeyConditionExpression += `${KeyConditionExpression ? ' AND ' : ''}#${name} = :${name}`;
            } else {
                FilterExpression += `${FilterExpression ? ' AND ' : ''}#${name} = :${name}`;
            }

            ExpressionAttributeNames[`#${name}`] = name;
            ExpressionAttributeValues[`:${name}`] = this.input(condition[name]);
        }

        const data = { TableName, KeyConditionExpression, ExpressionAttributeNames, ExpressionAttributeValues };

        if (IndexName) data.IndexName = IndexName;
        if (FilterExpression) data.FilterExpression = FilterExpression;

        const { Items } = await this.dynamo.query(data).promise();
        return Items.map(item => item ? this.unmarshall(item) : null);
    }

    async scan (condition) {
        if (!condition) throw new Error('condition required');

        const TableName = this.name;
        const ExpressionAttributeNames = {};
        const ExpressionAttributeValues = {};

        let IndexName;
        let FilterExpression = '';

        for (const name in condition) {
            const value = condition[name];
            if (value === undefined) continue;

            const schema = this.schema.get(name);

            if (schema) {
                if (schema.gsi && schema.hash === name) IndexName = schema.gsi;
            } else {
                FilterExpression += `${FilterExpression ? ' AND ' : ''}#${name} = :${name}`;
            }

            ExpressionAttributeNames[`#${name}`] = name;
            ExpressionAttributeValues[`:${name}`] = this.input(condition[name]);
        }

        const data = { TableName, ExpressionAttributeNames, ExpressionAttributeValues };

        if (IndexName) data.IndexName = IndexName;
        if (FilterExpression) data.FilterExpression = FilterExpression;

        const { Items } = await this.dynamo.scan(data).promise();
        return Items.map(item => item ? this.unmarshall(item) : null);
    }

};
