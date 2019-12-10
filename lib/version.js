'use stric';

module.exports = function (options) {
    if (options.version) options.apiVersion = options.version;
    return options;
};
