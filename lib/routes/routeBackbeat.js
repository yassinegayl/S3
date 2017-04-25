const url = require('url');
const async = require('async');

const { auth, errors } = require('arsenal');
const vault = require('../auth/vault');
const metadata = require('../metadata/wrapper');
const locationConstraintCheck = require(
    '../api/apiUtils/object/locationConstraintCheck');
const { dataStore } = require('../api/apiUtils/object/storeObject');
const { prepareRequestContexts } = require(
    '../api/apiUtils/authorization/prepareRequestContexts');
const { metadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const { responseXMLBody } = require('./routesUtils');

auth.setHandler(vault);

const NAMESPACE = 'default';
const CIPHER = null; // replication/lifecycle does not work on encrypted objects

function _parseRequest(req) {
    const pathname = url.parse(req.url, true).pathname;
    const pathArr = pathname.split('/');
    return {
        bucket: pathArr[3],
        object: pathArr[4],
        resourceType: pathArr[5],
    };
}

function _respond(response, payload, log, callback) {
    const body = typeof payload === 'object' ?
        JSON.stringify(payload) : payload;
    const httpHeaders = {
        'x-amz-id-2': log.getSerializedUids(),
        'x-amz-request-id': log.getSerializedUids(),
        'content-type': 'application/json',
        'content-length': Buffer.byteLength(body),
    };
    response.writeHead(200, httpHeaders);
    response.end(body, 'utf8', callback);
}

function _getRequestPayload(req, cb) {
    const payload = [];
    let payloadLen = 0;
    req.on('data', chunk => {
        payload.push(chunk);
        payloadLen += chunk.length;
    }).on('error', cb)
    .on('end', () => cb(null, Buffer.concat(payload, payloadLen).toString()));
}

/*
PUT /_/backbeat/<bucket name>/<object key>/metadata
PUT /_/backbeat/<bucket name>/<object key>/data
*/

function putData(request, response, bucketInfo, objMd, log, callback) {
    const { bucket, object } = _parseRequest(request);

    const canonicalID = request.headers['x-scal-canonicalId'];
    const contentMd5 = request.headers['content-md5'];
    const context = {
        bucketName: bucket,
        owner: canonicalID,
        namespace: NAMESPACE,
        objectKey: object,
    };
    const payloadLen = parseInt(request.headers['content-length'], 10);
    const backendInfoObj = locationConstraintCheck(
        request, null, bucketInfo, log);
    if (backendInfoObj.err) {
        log.error('error getting backendInfo', {
            error: backendInfoObj.err,
            method: 'routeBackbeat',
        });
        return callback(errors.InternalError);
    }
    const backendInfo = backendInfoObj.backendInfo;
    return dataStore(
        context, CIPHER, request, payloadLen, {},
        backendInfo, log, (err, retrievalInfo, md5) => {
            if (err) {
                return callback(err);
            }
            if (contentMd5 !== md5) {
                return callback(errors.BadDigest);
            }
            const { key, dataStoreName } = retrievalInfo;
            const dataRetrievalInfo = [{
                key,
                dataStoreName,
                size: payloadLen,
                start: 0,
            }];
            return _respond(response, dataRetrievalInfo, log, callback);
        });
}

function putMetadata(request, response, bucketInfo, objMd, log, callback) {
    const { bucket, object } = _parseRequest(request);

    return _getRequestPayload(request, (err, payload) => {
        let omVal;
        try {
            omVal = JSON.parse(payload);
        } catch (err) {
            // FIXME: add error type MalformedJSON
            return callback(errors.MalformedPOSTRequest);
        }
        // specify both 'versioning' and 'versionId' to create a "new"
        // version (updating master as well) but with specified
        // versionId
        const options = {
            versioning: true,
            versionId: omVal.versionId,
        };
        log.trace('putting object version', { object, omVal, options });
        return metadata.putObjectMD(
            bucket, object, omVal, options, log, (err, md) => {
                if (err) {
                    return callback(err);
                }
                return _respond(response, md, log, callback);
            });
    });
}


const backbeatRoutes = {
    PUT: { data: putData,
           metadata: putMetadata },
};

function routeBackbeat(request, response, log) {
    log.debug('routing request', { method: 'routeBackbeat' });
    const { bucket, object, resourceType } = _parseRequest(request);
    const invalidRequest = !bucket || !object || !resourceType;
    if (invalidRequest) {
        log.debug('invalid request', { method: request.method,
                                       bucket, object, resourceType });
        return responseXMLBody(errors.MethodNotAllowed, null, response, log);
    }
    const requestContexts = prepareRequestContexts('objectReplicate',
                                                   request);
    return async.waterfall([next => auth.server.doAuth(
        request, log, (err, userInfo) => {
            if (err) {
                log.debug('authentication error',
                          { error: err,
                            method: request.method, bucket, object });
            }
            return next(err, userInfo);
        }, 's3', requestContexts), (userInfo, next) => {
            const mdValParams = { bucketName: bucket,
                                  objectKey: object,
                                  authInfo: userInfo,
                                  requestType: 'ReplicateObject' };
            return metadataValidateBucketAndObj(mdValParams, log, next);
        }, (bucketInfo, objMd, next) => {
            if (backbeatRoutes[request.method] === undefined ||
                backbeatRoutes[request.method][resourceType] === undefined) {
                log.debug('no such route', { method: request.method,
                                             bucket, object, resourceType });
                return next(errors.MethodNotAllowed);
            }
            return backbeatRoutes[request.method][resourceType](
                request, response, bucketInfo, objMd, log, next);
        }],
        err => {
            if (err) {
                return responseXMLBody(err, null, response, log);
            }
            log.debug('backbeat route response sent successfully',
                      { method: request.method, bucket, object });
            return undefined;
        });
}


module.exports = routeBackbeat;
