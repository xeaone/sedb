const Credential = require('./credential.js');
const Version = require('./version.js');
const Aws = require('aws-sdk');

const join = function (data) {
    return typeof data === 'string' ? data : data.join('_');
};

module.exports = class Dynamo {

    constructor (options = {}) {

        this.name = options.name;
        this.primary = options.primary;
        this.queryOptions = options.query || {};
        this.pagination = typeof options.pagination === 'boolean' ? options.pagination : true;

        this.read = options.read || 1,
            this.write = options.write || 1;
        this.indexes = options.indexes || [];

        this.input = Aws.DynamoDB.Converter.input;
        this.output = Aws.DynamoDB.Converter.output;
        this.marshall = Aws.DynamoDB.Converter.marshall;
        this.unmarshall = Aws.DynamoDB.Converter.unmarshall;

        delete options.name;
        delete options.read;
        delete options.write;
        delete options.indexes;
        delete options.primary;

        if (typeof this.name !== 'string') throw new Error('name required');
        if (typeof this.primary !== 'object') throw new Error('primary required');

        Version(options);
        Credential(options);

        this.dynamo = new Aws.DynamoDB(options);
    }

    async mergeCondition (data, condition) {

        if (typeof data !== 'object') throw new Error('data object required');
        if (typeof condition !== 'object') throw new Error('condition object or array required');

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
                    data.ExpressionAttributeNames[ `#${item.name}` ] = item.name;
                    data.ExpressionAttributeValues[ `:${item.name}` ] = this.input(item.value);
                }
            }
        } else {
            for (const name in condition) {
                data.ConditionExpression += data.ConditionExpression ? ' AND ' : '';
                data.ConditionExpression += `#${name}=:${name}`;
                data.ExpressionAttributeNames[ `#${name}` ] = name;
                data.ExpressionAttributeValues[ `:${name}` ] = this.input(condition[ name ]);
            }
        }

    }

    async key ({ hash, range }) {
        const KeySchema = [];
        const AttributeDefinitions = [];

        if (hash) {
            KeySchema.push({ KeyType: 'HASH', AttributeName: hash });
            AttributeDefinitions.push({ AttributeType: 'S', AttributeName: hash });
        }

        if (range) {
            KeySchema.push({ KeyType: 'RANGE', AttributeName: range });
            AttributeDefinitions.push({ AttributeType: 'S', AttributeName: range });
        }

        return [ KeySchema, AttributeDefinitions ];
    }

    async setup () {

        const [ KeySchema, AttributeDefinitions ] = await this.key(this.primary);

        const data = {
            KeySchema,
            AttributeDefinitions,
            TableName: this.name,
            ProvisionedThroughput: {
                ReadCapacityUnits: this.read,
                WriteCapacityUnits: this.write
            }
        };

        if (this.indexes && this.indexes.length) {
            data.GlobalSecondaryIndexes = [];
            for (const index of this.indexes) {

                const [ KeySchema, AttributeDefinitions ] = await this.key(index);

                const hasHash = data.AttributeDefinitions.find(({ AttributeName }) => AttributeName === index.hash);
                const hasRange = data.AttributeDefinitions.find(({ AttributeName }) => AttributeName === index.range);

                if (!hasHash) data.AttributeDefinitions.push(AttributeDefinitions[ 0 ]);
                if (!hasRange) data.AttributeDefinitions.push(AttributeDefinitions[ 1 ]);

                data.GlobalSecondaryIndexes.push({
                    KeySchema,
                    IndexName: index.hash,
                    Projection: { ProjectionType: 'ALL' },
                    ProvisionedThroughput: {
                        ReadCapacityUnits: index.read || this.read,
                        WriteCapacityUnits: index.write || this.write
                    }
                });

            }
        }

        try {
            await this.dynamo.createTable(data).promise();
            await this.dynamo.waitFor('tableExists', { TableName: data.TableName }).promise();
        } catch (error) {
            if (error.code !== 'ResourceInUseException') throw error;
        }

    }

    // replaces existing item or creates new item.
    async put (item) {
        if (!item) throw new Error('item required');

        const Item = {};
        const TableName = this.name;
        const { hash, range } = this.primary;

        for (const name in item) {
            const value = item[ name ];
            if (value === undefined) continue;
            if (name === hash || name === range) {
                Item[ name ] = this.input(join(value));
            } else {
                Item[ name ] = this.input(value);
            }
        }

        const data = { Item, TableName };
        await this.dynamo.putItem(data).promise();
    }

    async get (item) {
        if (!item) throw new Error('item required');

        const Key = {};
        const TableName = this.name;
        const { hash, range } = this.primary;

        for (const name in item) {
            const value = item[ name ];
            if (value === undefined) continue;
            if (name === hash || name === range) {
                Key[ name ] = this.input(join(value));
            } else {
                Key[ name ] = this.input(value);
            }
        }

        const data = { TableName, Key };
        const { Item } = await this.dynamo.getItem(data).promise();
        const unmarshall = Item ? this.unmarshall(Item) : null;

        return unmarshall;
    }

    // removes item if it exists
    async remove (item, options) {
        if (!item) throw new Error('item required');

        const Key = {};
        const TableName = this.name;
        const { hash, range } = this.primary;

        for (const name in item) {
            const value = item[ name ];
            if (value === undefined) continue;
            if (name === hash || name === range) {
                Key[ name ] = this.input(join(value));
            } else {
                Key[ name ] = this.input(value);
            }
        }

        const data = { TableName, Key };

        if (typeof options === 'object' && typeof options.condition === 'object') {
            await this.mergeCondition(data, options.condition);
        }

        const { Item } = await this.dynamo.deleteItem(data).promise();
        const unmarshall = Item ? this.unmarshall(Item) : null;

        return unmarshall;
    }

    // adds a item if it does not exist and the condition is true
    async add (item, options) {
        if (!item) throw new Error('item required');

        const Item = {};
        const TableName = this.name;
        const ExpressionAttributeNames = {};
        const { hash, range } = this.primary;

        let ConditionExpression = '';

        // verify item does not exists
        for (const name in item) {
            const value = item[ name ];
            if (value === undefined) continue;

            if (name === hash || name === range) {
                ExpressionAttributeNames[ `#${name}` ] = name;
                Item[ name ] = this.input(join(value));
                ConditionExpression += `${ConditionExpression ? ' AND ' : ''}attribute_not_exists(#${name})`;
            } else {
                Item[ name ] = this.input(value);
            }

        }

        const data = { Item, TableName, ConditionExpression, ExpressionAttributeNames };

        if (typeof options === 'object' && typeof options.condition === 'object') {
            await this.mergeCondition(data, options.condition);
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
        const { hash, range } = this.primary;

        let SetExpression = '';
        let RemoveExpression = '';
        let UpdateExpression = '';
        let ConditionExpression = '';

        for (const name in item) {
            const value = item[ name ];

            // verify item exists
            if (name === hash || name === range) {
                Key[ name ] = this.input(join(value));
                ExpressionAttributeNames[ `#${name}` ] = name;
                ConditionExpression += `${ConditionExpression ? ' AND ' : ''}attribute_exists(#${name})`;
                continue;
            }

            ExpressionAttributeNames[ `#${name}` ] = name;

            if (value === undefined) {
                RemoveExpression += `${RemoveExpression ? ',' : 'REMOVE'} #${name}`;
            } else {
                ExpressionAttributeValues[ `:${name}` ] = this.input(value);
                SetExpression += `${SetExpression ? ',' : 'SET'} #${name} = :${name}`;
            }

        }

        UpdateExpression += `${SetExpression} ${RemoveExpression}`;

        const data = {
            Key,
            TableName,
            UpdateExpression,
            ConditionExpression,
            ExpressionAttributeNames
        };

        if (Object.keys(ExpressionAttributeValues).length) {
            data.ExpressionAttributeValues = ExpressionAttributeValues;
        }

        if (typeof options === 'object' && typeof options.condition === 'object') {
            await this.mergeCondition(data, options.condition);
        }

        // Dynamo.updateItem: Edits an existing item or adds a new item if it does not exist
        await this.dynamo.updateItem(data).promise();
    }

    async paginate (type, data, result) {
        result = result || [];

        const { Items, LastEvaluatedKey } = await this.dynamo[ type ](data).promise();

        Items.forEach(item => result.push(item ? this.unmarshall(item) : null));

        if (LastEvaluatedKey) {
            data.ExclusiveStartKey = LastEvaluatedKey;
            await this.paginate(type, data, result);
        }

        return result;
    }

    async query (condition, options) {
        if (!condition) throw new Error('condition required');

        options = options || this.queryOptions;

        const TableName = this.name;
        const ExpressionAttributeNames = {};
        const ExpressionAttributeValues = {};
        const { hash, range } = this.primary;

        let IndexName;
        let FilterExpression = '';
        let KeyConditionExpression = '';

        for (const name in condition) {
            const value = condition[ name ];
            if (value === undefined) continue;

            const index = this.indexes.find(({ hash }) => hash === name);

            if (name === hash || name === range || index) {
                if (index) IndexName = name;

                if (name === range && options && options.begins === true) {
                    KeyConditionExpression += `${KeyConditionExpression ? ' AND ' : ''}begins_with( #${name}, :${name} )`;
                } else {
                    KeyConditionExpression += `${KeyConditionExpression ? ' AND ' : ''}#${name} = :${name}`;
                }

                ExpressionAttributeValues[ `:${name}` ] = this.input(join(condition[ name ]));
            } else {
                ExpressionAttributeValues[ `:${name}` ] = this.input(condition[ name ]);
                FilterExpression += `${FilterExpression ? ' AND ' : ''}#${name} = :${name}`;
            }

            ExpressionAttributeNames[ `#${name}` ] = name;
        }

        const data = { TableName, KeyConditionExpression, ExpressionAttributeNames, ExpressionAttributeValues };

        if (IndexName) data.IndexName = IndexName;
        if (FilterExpression) data.FilterExpression = FilterExpression;

        let result;

        if (this.pagination) {
            result = await this.paginate('query', data);
        } else {
            const { Items } = await this.dynamo.query(data).promise();
            result = Items.map(item => item ? this.unmarshall(item) : null);
        }

        return result;
    }

    async scan (condition) {
        if (!condition) throw new Error('condition required');

        const TableName = this.name;
        const ExpressionAttributeNames = {};
        const ExpressionAttributeValues = {};
        const { hash, range } = this.primary;

        let IndexName;
        let FilterExpression = '';

        for (const name in condition) {
            const value = condition[ name ];
            if (value === undefined) continue;

            const index = this.indexes.find(({ hash }) => hash === name);

            if (name === hash || name === range || index) {
                if (index) IndexName = name;
                ExpressionAttributeValues[ `:${name}` ] = this.input(join(condition[ name ]));
            } else {
                ExpressionAttributeValues[ `:${name}` ] = this.input(condition[ name ]);
                FilterExpression += `${FilterExpression ? ' AND ' : ''}#${name} = :${name}`;
            }

            ExpressionAttributeNames[ `#${name}` ] = name;
        }

        const data = { TableName, ExpressionAttributeNames, ExpressionAttributeValues };

        if (IndexName) data.IndexName = IndexName;
        if (FilterExpression) data.FilterExpression = FilterExpression;

        let result;

        if (this.pagination) {
            result = await this.paginate('scan', data);
        } else {
            const { Items } = await this.dynamo.query(data).promise();
            result = Items.map(item => item ? this.unmarshall(item) : null);
        }

        return result;
    }

};
