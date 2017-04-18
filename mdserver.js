'use strict'; // eslint-disable-line strict
require('babel-core/register');

const config = require('./lib/Config.js').default;
const MetadataServer =
          require('arsenal').storage.metadata.MetadataFileServer;

if (config.backends.metadata === 'file') {
    /**
     * Configure the file path for metadata if using the file backend.
     * If no path provided, uses data and metadata at the root of the
     * S3 project directory
     */
    const metadataPath = process.env.S3METADATAPATH ?
              process.env.S3METADATAPATH : `${__dirname}/localMetadata`;
    const mdServer = new MetadataServer(
        { metadataPath, metadataPort: config.metadataDaemon.port,
          log: config.log });
    mdServer.startServer();
}

