'use strict'; // eslint-disable-line strict
require('babel-core/register');

const config = require('./lib/Config.js').default;
const MetadataServer = require('arsenal').storage.metadata.server;

if (config.backends.metadata === 'file') {
    const recordLogPath = process.env.S3RECORDLOGPATH ?
              process.env.S3RECORDLOGPATH : `${__dirname}/localRecordLog`;
    const mdServer = new MetadataServer(
        { metadataPath: config.filePaths.metadataPath,
          metadataPort: config.metadataDaemon.port,
          recordLogEnabled: true, //FIXME
          recordLogPath, log: config.log });
    mdServer.startServer();
}

