import cluster from 'cluster';

import arsenal from 'arsenal';

import { logger } from '../../utilities/logger';
import BucketInfo from '../BucketInfo';
import constants from '../../../constants';
import config from '../../Config';

const errors = arsenal.errors;
const mdServer = arsenal.storage.metadata.server;
const mdClient = arsenal.storage.metadata.client;

const METADATA_PORT = 9990;
const METADATA_PATH = `${config.filePaths.metadataPath}/`;
const MANIFEST_JSON = 'manifest.json';
const METASTORE = '__metastore';
const OPTIONS = { sync: true };

class BucketFileInterface {

    constructor() {
        this.logger = logger;
        if (cluster.isMaster) {
            this.mdServer = new mdServer(
            { metadataPath: METADATA_PATH,
              metadataPort: METADATA_PORT,
              manifestJson: MANIFEST_JSON,
              logger });
            this.mdServer.startServer();
            this.setupMetadataServer();
        }
        this.mdClient = new mdClient(
            { metadataPath: METADATA_PATH,
              metadataPort: METADATA_PORT,
              manifestJson: MANIFEST_JSON,
              logger });
        this.mdClient.connectDb();
        this.metastore = this.mdClient.openSub(METASTORE, logger);
    }

    setupMetadataServer() {
        const metastore = this.mdServer.openSub(METASTORE);
        /* Since the bucket creation API is expecting the
           usersBucket to have attributes, we pre-create the
           usersBucket here */
        this.mdServer.createSub(constants.usersBucket, err => {
            if (err) {
                this.logger.fatal('error creating usersBucket',
                                  { error: err });
                throw (errors.InternalError);
            }
        });
        const usersBucketAttr = new BucketInfo(constants.usersBucket,
            'admin', 'admin', new Date().toJSON(),
            BucketInfo.currentModelVersion());
        metastore.put(constants.usersBucket, usersBucketAttr.serialize());
        const stream = metastore.createKeyStream();
        stream
            .on('data', e => {
                // automatically recreate existing sublevels
                this.mdServer.createSub(e, err => {
                    if (err) {
                        this.logger.fatal(
                            `error creating sublevel for bucket ${e}`,
                                          { error: err });
                        throw (errors.InternalError);
                    }
                });
            })
            .on('error', err => {
                this.logger.fatal('error listing metastore', { error: err });
                throw (errors.InternalError);
            })
            .on('end', () => {
                this.logger.debug('finished creating sublevels');
            });
    }

    /**
     * Load DB if exists
     * @param {String} bucketName - name of bucket
     * @param {Object} log - logger
     * @param {function} cb - callback(err, db, attr)
     * @return {undefined}
     */
    loadDBIfExists(bucketName, log, cb) {
        this.getBucketAttributes(bucketName, log, (err, attr) => {
            if (err) {
                return cb(err, null);
            }
            const db = this.mdClient.openSub(bucketName, log);
            return cb(null, db, attr);
        });
        return undefined;
    }

    createBucket(bucketName, bucketMD, log, cb) {
        this.getBucketAttributes(bucketName, log, err => {
            if (err && err !== errors.NoSuchBucket) {
                return cb(err);
            }
            if (err === undefined) {
                return cb(errors.BucketAlreadyExists);
            }
            this.mdClient.createSub(bucketName, log, err => {
                if (err) {
                    log.error(`error creating bucket ${bucketName}`,
                              { error: err });
                    return cb(errors.InternalError);
                }
                this.putBucketAttributes(bucketName,
                                         bucketMD,
                                         log, cb);
                return undefined;
            });
            return undefined;
        });
        return undefined;
    }

    getBucketAttributes(bucketName, log, cb) {
        this.metastore.get(bucketName, (err, data) => {
            if (err) {
                if (err.notFound) {
                    return cb(errors.NoSuchBucket);
                }
                log.error('error getting db attributes',
                          { error: err });
                return cb(errors.InternalError, null);
            }
            return cb(null, BucketInfo.deSerialize(data));
        });
        return undefined;
    }

    getBucketAndObject(bucketName, objName, log, cb) {
        this.loadDBIfExists(bucketName, log, (err, db, bucketAttr) => {
            if (err) {
                return cb(err);
            }
            db.get(objName, (err, objAttr) => {
                if (err) {
                    if (err.notFound) {
                        return cb(null, {
                            bucket: bucketAttr.serialize(),
                        });
                    }
                    log.error('error getting object', { error: err });
                    return cb(errors.InternalError);
                }
                return cb(null, {
                    bucket: bucketAttr.serialize(),
                    obj: objAttr,
                });
            });
            return undefined;
        });
        return undefined;
    }

    putBucketAttributes(bucketName, bucketMD, log, cb) {
        this.metastore.put(bucketName, bucketMD.serialize(),
                           OPTIONS,
                           err => {
                               if (err) {
                                   log.error('error putting db attributes',
                                             { error: err });
                                   return cb(errors.InternalError);
                               }
                               return cb();
                           });
        return undefined;
    }

    deleteBucket(bucketName, log, cb) {
        this.metastore.del(bucketName,
                           err => {
                               if (err) {
                                   log.error('error deleting bucket',
                                             { error: err });
                                   return cb(errors.InternalError);
                               }
                               return cb();
                           });
        return undefined;
    }

    putObject(bucketName, objName, objVal, log, cb) {
        this.loadDBIfExists(bucketName, log, (err, db) => {
            if (err) {
                return cb(err);
            }
            db.put(objName, JSON.stringify(objVal),
                   OPTIONS, err => {
                       if (err) {
                           log.error('error putting object',
                                     { error: err });
                           return cb(errors.InternalError);
                       }
                       return cb();
                   });
            return undefined;
        });
    }

    getObject(bucketName, objName, log, cb) {
        this.loadDBIfExists(bucketName, log, (err, db) => {
            if (err) {
                return cb(err);
            }
            db.get(objName, (err, data) => {
                if (err) {
                    if (err.notFound) {
                        return cb(errors.NoSuchKey);
                    }
                    log.error('error getting object',
                              { error: err });
                    return cb(errors.InternalError);
                }
                return cb(null, JSON.parse(data));
            });
            return undefined;
        });
    }

    deleteObject(bucketName, objName, log, cb) {
        this.loadDBIfExists(bucketName, log, (err, db) => {
            if (err) {
                return cb(err);
            }
            db.del(objName, OPTIONS, err => {
                if (err) {
                    log.error('error deleting object',
                              { error: err });
                    return cb(errors.InternalError);
                }
                return cb();
            });
            return undefined;
        });
    }

    /**
     *  This function checks if params have a property name
     *  If there is add it to the finalParams
     *  Else do nothing
     *  @param {String} name - The parameter name
     *  @param {Object} params - The params to search
     *  @param {Object} extParams - The params sent to the extension
     *  @return {undefined}
     */
    addExtensionParam(name, params, extParams) {
        if (params.hasOwnProperty(name)) {
            // eslint-disable-next-line no-param-reassign
            extParams[name] = params[name];
        }
    }

    /**
     * Used for advancing the last character of a string for setting upper/lower
     * bounds
     * For e.g., _setCharAt('demo1') results in 'demo2',
     * _setCharAt('scality') results in 'scalitz'
     * @param {String} str - string to be advanced
     * @return {String} - modified string
     */
    _setCharAt(str) {
        let chr = str.charCodeAt(str.length - 1);
        chr = String.fromCharCode(chr + 1);
        return str.substr(0, str.length - 1) + chr;
    }

    /**
     *  This complex function deals with different extensions of bucket listing:
     *  Delimiter based search or MPU based search.
     *  @param {String} bucketName - The name of the bucket to list
     *  @param {Object} params - The params to search
     *  @param {Object} log - The logger object
     *  @param {function} cb - Callback when done
     *  @return {undefined}
     */
    internalListObject(bucketName, params, log, cb) {
        const requestParams = {};
        let Ext;
        const extParams = {};
        // multipart upload listing
        if (params.listingType === 'multipartuploads') {
            Ext = arsenal.algorithms.list.MPU;
            this.addExtensionParam('queryPrefixLength', params, extParams);
            this.addExtensionParam('splitter', params, extParams);
            if (params.keyMarker) {
                requestParams.gt = `overview${params.splitter}` +
                    `${params.keyMarker}${params.splitter}`;
                if (params.uploadIdMarker) {
                    requestParams.gt += `${params.uploadIdMarker}`;
                }
                // advance so that lower bound does not include the supplied
                // markers
                requestParams.gt = this._setCharAt(requestParams.gt);
            }
        } else {
            Ext = arsenal.algorithms.list.Delimiter;
            if (params.marker) {
                requestParams.gt = params.marker;
                this.addExtensionParam('gt', requestParams, extParams);
            }
        }
        this.addExtensionParam('delimiter', params, extParams);
        this.addExtensionParam('maxKeys', params, extParams);
        if (params.prefix) {
            requestParams.start = params.prefix;
            requestParams.lt = this._setCharAt(params.prefix);
            this.addExtensionParam('start', requestParams, extParams);
        }
        const extension = new Ext(extParams, log);
        this.loadDBIfExists(bucketName, log, (err, db) => {
            if (err) {
                return cb(err);
            }
            let cbDone = false;
            const stream = db.createReadStream(requestParams);
            stream
                .on('data', e => {
                    if (extension.filter(e) === false) {
                        stream.emit('end');
                        stream.destroy();
                    }
                })
                .on('error', err => {
                    if (!cbDone) {
                        cbDone = true;
                        log.error('error listing objects',
                                  { error: err });
                        cb(errors.InternalError);
                    }
                })
                .on('end', () => {
                    if (!cbDone) {
                        cbDone = true;
                        const data = extension.result();
                        cb(null, data);
                    }
                });
            return undefined;
        });
    }

    listObject(bucketName, params, log, cb) {
        return this.internalListObject(bucketName, params, log, cb);
    }

    listMultipartUploads(bucketName, params, log, cb) {
        return this.internalListObject(bucketName, params, log, cb);
    }
}

export default BucketFileInterface;
