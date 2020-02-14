const Aws = require('aws-sdk');

module.exports = function (options) {

    if (!options.credentials && options.profile) {
        options.credentials = new Aws.SharedIniFileCredentials({ profile: options.profile });
        delete options.profile;
    }

    if (!options.credentials && options.accessKeyId && options.secretAccessKey) {
        options.credentials = new Aws.Credentials({ accessKeyId: options.accessKeyId, secretAccessKey: options.secretAccessKey });
        delete options.accessKeyId;
        delete options.secretAccessKey;
    }

    return options;
};
