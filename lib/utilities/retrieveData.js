const EventEmitter = require('events');

class RetrieveMeEmitter extends EventEmitter {
    constructor(location) {
        super();
        this.location = location;
        this.readable = undefined;
    }

    setReadable(readable) {
        this.readable = readable;
        return;
    }

    getReadable() {
        return this.readable;
    }
}

function _sendReadableToResponse(dataStreams, index, errorHandlerFn,
                                  response, logger) {
    if (dataStreams[index] === undefined) {
        // eslint-disable-next-line
        // dataStreams = null;
        return response.end();
    }
    const emitterOnCall = dataStreams[index];
    return emitterOnCall.on('readMe', () => {
        const readable = emitterOnCall.getReadable();
        readable.on('data', chunk => {
            response.write(chunk);
        });
        readable.on('error', err => {
            logger.error('error piping data from source');
            errorHandlerFn(err);
        });
        readable.on('end', () => {
            process.nextTick(_sendReadableToResponse,
                dataStreams, index + 1, errorHandlerFn, response, logger);
        });
    });
}

function _connectToData(dataStreams, index, dataRetrievalFn, errorHandlerFn,
                        response, logger) {
    return dataRetrievalFn(dataStreams[index].location, logger,
        (err, readable) => {
            if (err) {
                logger.error('failed to get object', {
                    error: err,
                    method: '_connectToData',
                });
                return errorHandlerFn(err);
            }
            const currentRetrieveMeEmitter = dataStreams[index];
            currentRetrieveMeEmitter.setReadable(readable);
            currentRetrieveMeEmitter.emit('readMe');
            if (dataStreams[index + 2]) {
                return process.nextTick(_connectToData, dataStreams,
                    index + 2,
                    dataRetrievalFn, errorHandlerFn, response, logger);
            }
            return undefined;
        });
}

function retrieveData(locations, dataRetrievalFn,
    response, logger, errorHandlerFn) {
    if (locations.length === 0) {
        return response.end();
    }
    if (errorHandlerFn === undefined) {
        // eslint-disable-next-line
        errorHandlerFn = () => { response.connection.destroy(); };
    }
    const dataStreams = locations.map(location => {
        const stream = new RetrieveMeEmitter(location);
        return stream;
    });

    _sendReadableToResponse(dataStreams, 0, errorHandlerFn,
                             response, logger);
    _connectToData(dataStreams, 0, dataRetrievalFn, errorHandlerFn,
                   response, logger);
    if (dataStreams.length > 1) {
        _connectToData(dataStreams, 1, dataRetrievalFn, errorHandlerFn,
                       response, logger);
    }
    return undefined;
}

module.exports = retrieveData;
