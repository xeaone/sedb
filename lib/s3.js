const Credential = require('./credential.js');
const Version = require('./version.js');
const Aws = require('aws-sdk');

module.exports = class S3 {

    constructor (options = {}) {

        this.name = options.name;
        this.acl = options.acl || 'private';
        this.parse = typeof options.parse !== 'boolean' ? false : options.parse;
        this.stringify = typeof options.stringify !== 'boolean' ? false : options.stringify;

        delete options.acl;
        delete options.name;
        delete options.parse;
        delete options.stringify;

        if (typeof this.name !== 'string') throw new Error('name required');

        Version(options);
        Credential(options);

        this.s3 = new Aws.S3(options);
    }

    async setup () {

        try {
            await this.s3.createBucket({ ACL: this.acl, Bucket: this.name }).promise();
        } catch (error) {
            if (
                error.code !== 'BucketAlreadyOwnedByYou' &&
                error.code !== 'BucketAlreadyExists'
            ) {
                throw error;
            }
        }

    }

    async list (key, options = {}) {
        if (!key) throw new Error('key required');

        let result;

        try {
            result = await this.s3.listObjectsV2({
                MaxKeys: 1000,
                Prefix: key,
                Bucket: this.name,
                ContinuationToken: options.token
            }).promise();
        } catch (error) {
            if (error.code !== 'NoSuchBucket') {
                throw error;
            }
        }

        let body;

        if (result) {
            body = result.Contents.map(({ Key }) => Key);
        }

        return {
            body: body,
            token: result.NextContinuationToken
        };
    }

    async has (key) {
        if (!key) throw new Error('key required');

        try {
            await this.s3.headObject({
                Key: key,
                Bucket: this.name
            }).promise();
        } catch (error) {
            if (error.code === 'NotFound') {
                return false;
            } else {
                throw error;
            }
        }

        return true;
    }

    async remove (key) {
        if (!key) throw new Error('key required');

        let result = null;

        try {
            result = await this.s3.deleteObject({
                Key: key,
                Bucket: this.name
            }).promise();
        } catch (error) {
            if (error.code !== 'NoSuchKey') {
                throw error;
            }
        }

        return result;
    }

    async get (key, options = {}) {
        if (!key) throw new Error('key required');

        let result;

        try {
            result = await this.s3.getObject({
                Key: key,
                Bucket: this.name
            }).promise();
        } catch (error) {
            if (error.code !== 'NoSuchKey') {
                throw error;
            }
        }

        let body;

        if (result) {
            const parse = typeof options.parse === 'boolean' ? this.parse : false;
            body = result.Body;
            if (parse) body = JSON.parse(body);
        }

        return body;
    }

    async set (key, body, options = {}) {
        if (!key) throw new Error('key required');
        if (!body) throw new Error('body required');

        if (typeof body === 'object') {
            const stringify = typeof options.stringify === 'boolean' ? options.stringify : this.stringify;
            if (stringify) body = JSON.stringify(body);
        }

        const result = await this.s3.putObject({
            Key: key,
            Body: body,
            Bucket: this.name
        }).promise();

        return result;
    }

};
