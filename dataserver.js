'use strict'; // eslint-disable-line strict
require('babel-core/register');

const arsenal = require('arsenal');
const config = require('./lib/Config.js').default;

if (config.backends.data === 'file') {
    const restServer = new arsenal.network.rest.Server(
        { port: config.dataDaemon.port,
          dataStore: new arsenal.storage.data.file.Store(
              { dataPath: config.dataDaemon.dataPath,
                log: config.log }),
          log: config.log });
    restServer.setup(() => {
        restServer.start();
    });
}
