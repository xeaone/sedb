'use stric';

const Aws = require('aws-sdk');
const Version = require('./version.js');
const Credential = require('./credential.js');

module.exports = class S3 {

    constructor (options = {}) {
        this.name = options.name;
        this.acl = options.acl || 'private';

        this.parse = typeof options.parse !== 'boolean' ? true : options.parse;
        this.encrypt = typeof options.encrypt !== 'boolean' ? true : options.encrypt;
        this.decrypt = typeof options.decrypt !== 'boolean' ? true : options.decrypt;
        this.stringify = typeof options.stringify !== 'boolean' ? true : options.stringify;

        delete options.name;
        delete options.acl;
        delete options.parse;
        delete options.encrypt;
        delete options.decrypt;
        delete options.stringify;

        if (typeof this.name !== 'string') throw new Error('name required');

        Version(options);
        Credential(options);

        this.s3 = new Aws.S3(options);
    }

    async setup (data = {}) {
        const acl = data.acl || this.acl;

        try {
            await this.s3.createBucket({ ACL: acl, Bucket: this.name }).promise();
        } catch (error) {
            if (error.code !== 'BucketAlreadyOwnedByYou') {
                throw error;
            }
        }

    }

    async list (data = {}) {
        if (!data.key) throw new Error('key missing');

        let result;

        try {
            result = await this.s3.listObjectsV2({
                MaxKeys: 1000,
                Prefix: data.key,
                Bucket: this.name,
                ContinuationToken: data.token
            }).promise();
        } catch (error) {
            if (error.code !== 'NoSuchBucket') {
                throw error;
            }
        }

        let body;

        if (result) {
            body = [];

            for (let content of result.Contents) {
                body.push(content);
            }

        }

        return {
            body: body,
            token: result.NextContinuationToken
        };
    }

    async has (data = {}) {
        if (!data.key) throw new Error('key missing');

        try {
            await this.s3.headObject({ Key: data.key, Bucket: this.name }).promise();
        } catch (error) {
            if (error.code === 'NotFound') {
                return false;
            } else {
                throw error;
            }
        }

        return true;
    }

    async remove (data = {}) {
        if (!data.key) throw new Error('key missing');

        let result = null;

        try {
            result = await this.s3.deleteObject({
                Key: data.key,
                Bucket: this.name
            }).promise();
        } catch (error) {
            if (error.code !== 'NoSuchKey') {
                throw error;
            }
        }

        return result;
    }

    async get (data = {}) {
        if (!data.key) throw new Error('key missing');

        let result;

        try {
            result = await this.s3.getObject({
                Key: data.key,
                Bucket: this.name
            }).promise();
        } catch (error) {
            if (error.code !== 'NoSuchKey') {
                throw error;
            }
        }

        let body;

        if (result) {
            const parse = data.parse === undefined ? this.parse : false;

            body = result.Body;

            // if (data.secret || this.secret && data.decrypt || this.decrypt) {
            // }

            if (parse) {
                body = JSON.parse(body);
            }

        }

        return body;
    }

    async set (data = {}) {
        if (!data.key) throw new Error('key missing');
        if (!data.body) throw new Error('body missing');

        let body;

        if (data.stringify || this.stringify && typeof data.body === 'object') {
            body = JSON.stringify(data.body);
        } else {
            body = data.body;
        }

        const result = await this.s3.putObject({
            Body: body,
            Key: data.key,
            Bucket: this.name
        }).promise();

        return result;
    }

};
