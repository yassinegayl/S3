import url from 'url';
import { readySetStream } from 'ready-set-stream';

import { auth, errors } from 'arsenal';
import vault from '../auth/vault';
import data from '../data/wrapper';
import metadata from '../metadata/wrapper';
import locationConstraintCheck from
    '../api/apiUtils/object/locationConstraintCheck';
import { dataStore } from '../api/apiUtils/object/storeObject';

import routesUtils from './routesUtils';
const { responseXMLBody } = routesUtils;

auth.setHandler(vault);

const NAMESPACE = 'default';
const CIPHER = null; // replication/lifecycle does not work on encrypted objects


function _parseRequest(req) {
    const pathname = url.parse(req.url, true).pathname;
    const pathArr = pathname.split('/');
    return {
        bucket: pathArr[3],
        object: pathArr[4],
        type: pathArr[5],
    };
}

function _respond(response, payload, log) {
    const body = typeof payload === 'object' ?
        JSON.stringify(payload) : payload;
    const httpHeaders = {
        'x-amz-id-2': log.getSerializedUids(),
        'x-amz-request-id': log.getSerializedUids(),
        'content-type': 'application/json',
        'content-length': Buffer.byteLength(body),
    };
    response.writeHead(200, httpHeaders);
    response.end(body, 'utf8', () => log.info('responded to request'));
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
GET /_/backbeat/<bucket name>/<object key>/metadata
PUT /_/backbeat/<bucket name>/<object key>/data
GET /_/backbeat/<bucket name>/<object key>/data
*/
export default function routeBackbeat(request, response, log) {
    log.debug('routing request', { method: 'routerBackbeat' });
    const { bucket, object, type } = _parseRequest(request);
    const validReqs = { metadata: true, data: true };
    const invalidRequest = !bucket || !object || !type || !validReqs[type];
    if (invalidRequest) {
        return responseXMLBody(errors.MethodNotAllowed, null, response, log);
    }
    // return auth.server.doAuth(request, log, err => {
    //     if (err) {
    //         log.trace('authentication error', { error: err });
    //         return responseXMLBody(err, null, response, log);
    //     }
        if (request.method === 'GET' && type === 'metadata') {
            return metadata.getObjectMD(bucket, object, {}, log, (err, md) => {
                if (err) {
                    return responseXMLBody(err, null, response, log);
                }
                return _respond(response, md, log);
            });
        }

        // get Data from the given data locations
        if (request.method === 'POST' && type === 'data') {
            return _getRequestPayload(request, (err, payload) => {
                const locations = JSON.parse(payload);
                return readySetStream(locations, data.get, response, log);
            });
        }

        if (request.method === 'PUT' && type === 'metadata') {
            return _getRequestPayload(request, (err, payload) => {
                const omVal = JSON.parse(payload);
                const options = {
                    versionId: omVal.versionId,
                };
                metadata.putObjectMD(bucket, object, omVal, options, log,
                    err => {
                        if (err) {
                            return responseXMLBody(err, null, response, log);
                        }
                        metadata.putObjectMD(bucket, object, omVal,
                            { versionId: '' }, log, (err, md) => {
                                return _respond(response, md, log);
                            });
                        return undefined;
                    });
            });
        }

        if (request.method === 'PUT' && type === 'data') {
            const canonicalID = request.headers['x-scal-canonicalId'];
            const contentMd5 = request.headers['content-md5'];
            const context = {
                bucketName: bucket,
                owner: canonicalID,
                namespace: NAMESPACE,
                objectKey: object,
            };
            const payloadLen = parseInt(request.headers['content-length'], 10);
            return metadata.getBucket(bucket, log, (err, bucketInfo) => {
                const backendInfoObj =
                    locationConstraintCheck(request, null, bucketInfo, log);
                if (backendInfoObj.err) {
                    log.error('error getting backendInfo', {
                        error: backendInfoObj.err,
                        method: 'routeBackbeat',
                    });
                    return responseXMLBody(errors.InternalError, response,
                        log);
                }
                const backendInfo = backendInfoObj.backendInfo;
                return dataStore(context, CIPHER, request, payloadLen, {},
                    backendInfo, log, (err, retrievalInfo, md5) => {
                        if (err) {
                            return responseXMLBody(err, null, response, log);
                        }
                        if (contentMd5 !== md5) {
                            return responseXMLBody(errors.BadDigest, null,
                                response, log);
                        }
                        const { key, dataStoreName } = retrievalInfo;
                        const dataRetrievalInfo = [{
                            key,
                            dataStoreName,
                            size: payloadLen,
                            start: 0,
                        }];
                        return _respond(response, dataRetrievalInfo, log);
                    });
            });
        }
        return responseXMLBody(errors.MethodNotAllowed, null, response, log);
    // }, 's3');
}
