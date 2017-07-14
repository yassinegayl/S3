const werelogs = require('werelogs');

const _config = require('../Config').config;

console.log("input loglevel is: ", _config.log.logLevel);

const logger = new werelogs.Werelogs('S3', {
    level: _config.log.logLevel,
    dump: _config.log.dumpLevel,
});

console.log("final logger is: ", logger.Logger);

module.exports = logger;
