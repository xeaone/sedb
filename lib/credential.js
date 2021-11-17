const { fromIni } = require("@aws-sdk/credential-provider-ini");

module.exports = function (options) {

    if (!options.credentials && options.profile) {
        options.credentials = fromIni({ profile: options.profile });
        delete options.profile;
    }

    if (!options.credentials && options.accessKeyId && options.secretAccessKey) {
        options.credentials = { accessKeyId: options.accessKeyId, secretAccessKey: options.secretAccessKey };
        delete options.accessKeyId;
        delete options.secretAccessKey;
    }

    return options;
};
