const werelogs = require('werelogs');
const { config } = require('../Config');

const logger = new werelogs.Werelogs('MultipleBackendGateway', {
    logLevel: config.log.logLevel,
    dumpLevel: config.log.dumpLevel,
});

function createLogger(reqUids) {
    return reqUids ?
        logger.newRequestLoggerFromSerializedUids(reqUids) :
        logger.newRequestLogger();
}

module.exports = createLogger;
