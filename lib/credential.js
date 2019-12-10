'use stric';

const Aws = require('aws-sdk');

module.exports = function (options) {

    if (options.credential) {
        options.Credentials = options.credential;
        delete options.credential;
    }

    if (options.credentials) {
        options.Credentials = options.credentials;
        delete options.credentials;
    }

    if (!options.Credentials && options.profile) {
        options.Credentials = new Aws.SharedIniFileCredentials({ profile: options.profile });
        delete options.profile;
    }

    if (!options.Credentials && options.accessKeyId && options.secretAccessKey) {
        options.Credentials = new Aws.Credentials({ accessKeyId: options.accessKeyId, secretAccessKey: options.secretAccessKey });
        delete options.accessKeyId;
        delete options.secretAccessKey;
    }

    return options;
};
