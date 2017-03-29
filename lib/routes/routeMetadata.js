import { errors } from 'arsenal';

import api from '../api/api';
import routesUtils from './routesUtils';
import metadata from '../metadata/wrapper';


function modifyChunk(chunk) {
    // eslint-disable-next-line no-param-reassign
    chunk.value = JSON.parse(chunk.value);
    // eslint-disable-next-line no-param-reassign
    chunk.userMd = {};
    for (let i = 0; i < Object.keys(chunk.value).length; i ++) {
        const key = Object.keys(chunk.value)[i];
        if (key.startsWith('x-amz-meta')) {
            // eslint-disable-next-line no-param-reassign
            chunk.userMd[key] = chunk.value[key];
            // eslint-disable-next-line no-param-reassign
            delete chunk.value[key];
        }
    }
    return `${JSON.stringify(chunk)}\n`;
}

/*
* Deals with requests to METADATA bucket to retrieve all of the object
* metadata for a bucket in order to do searches
* @param {object} clientIP - IP address of client
* @param {booleran} deep - true if healthcheck will check backends
* @param {object} req - http request object
* @param {object} res - http response object
* @param {object} log - werelogs logger instance
*/
export default function routeMetadata(req, res, log) {
    if (req.method.toUpperCase() === 'HEAD') {
        if (!req.objectKey) {
        // For now on bucket head request just return 200
        // Could add authentication check later but
        // it's a public bucket in a sense
            return routesUtils.responseNoBody(null, null, res,
            200, log);
        }
        if (req.objectKey.endsWith('_spark_metadata')
            || req.objectKey.endsWith('_spark_metadata/')) {
            // SPARK IS DOING A HEAD OBJECT
            // ON METADATA/bucketname/_spark_metadata
            // as well as METADATA/bucketname/_spark_metadata/
            // so send back 404 for both for now
            // TODO: determine whether to allow puts of these items to
            // allow for writes back from Spark (would need some bucket to
            // put them in)
            return routesUtils.responseNoBody(errors.NoSuchKey, null, res,
                200, log);
        }
        // Spark is making a head request on the object before actually
        // getting to obtain content length so need to fabricate head
        // response with content length
        return api.callApiMethod('getMetadata', req, res, log,
            (err, stream) => {
                if (err) {
                    return routesUtils.responseContentHeaders(err, {}, {},
                        res, log);
                }
                let contentLength = 0;
                // calculate length on stream
                stream.on('data', chunk => {
                    const modifiedChunk = modifyChunk(chunk);
                    contentLength += modifiedChunk.length;
                });
                stream.on('error', () => {
                    log.error('error streaming data from source');
                    return res.end();
                });
                stream.on('end', () => {
                    const resHeaders = {
                        'Content-Length': contentLength,
                    };
                    return routesUtils.responseContentHeaders(err, {},
                        resHeaders, res, log);
                });
                return undefined;
            });
    }
    if (req.method.toUpperCase() === 'GET') {
        // Spark does listing of
        // METADATA/
        // ?max-keys=1&prefix=bucketname%2F_spark_metadata%2F&delimiter=%2F
        // so should return empty LISTING for now
        if (req.objectKey === undefined) {
            const xml = [];
            xml.push(
                '<?xml version="1.0" encoding="UTF-8"?>',
                '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/' +
                    '2006-03-01/">',
                '<Name>METADATA</Name>'
                );
            const xmlParams = [
                { tag: 'Prefix', value: req.query.prefix },
                { tag: 'Marker', value: undefined },
                { tag: 'MaxKeys', value: req.query['max-keys'] },
                { tag: 'Delimiter', value: req.query.delimiter },
                { tag: 'IsTruncated', value: 'false' },
            ];

            xmlParams.forEach(p => {
                if (p.value) {
                    xml.push(`<${p.tag}>${p.value}</${p.tag}>`);
                } else {
                    xml.push(`<${p.tag}/>`);
                }
            });
            xml.push('</ListBucketResult>');
            return routesUtils.responseXMLBody(null, xml.join(''), res, log);
        }
        return api.callApiMethod('getMetadata', req, res, log,
            (err, streamForSize) => {
                // Currently streaming the object twice to get the content
                // length since the version of the java aws sdk being used
                // in spark docker is using the response header size to check
                // size. Hopefully, updating the aws sdk version will remove
                // this problem.
                if (err) {
                    return routesUtils.responseNoBody(err, null, res,
                        200, log);
                }
                let contentLength = 0;
                streamForSize.on('data', chunk => {
                    const modifiedChunk = modifyChunk(chunk);
                    contentLength += modifiedChunk.length;
                });
                streamForSize.on('error', () => {
                    log.error('error streaming data from source');
                    return res.end();
                });
                streamForSize.on('end', () => {
                    log.trace('obtained content length', { contentLength });
                    return metadata.getAllObjects(req.objectKey, log,
                        (err, stream) => {
                            if (err) {
                                return routesUtils.responseNoBody(err, null,
                                    res, 200, log);
                            }
                            res.setHeader('Content-Length', contentLength);
                            res.writeHead(200,
                                { 'Content-Type': 'application/json' });
                            // send stream to response
                            stream.on('data', chunk => {
                                const modifiedChunk = modifyChunk(chunk);
                                res.write(modifiedChunk);
                            });
                            stream.on('end', () => {
                                res.end();
                            });
                            return undefined;
                        });
                });
                return undefined;
            });
    }
    return routesUtils.responseNoBody(errors.MethodNotAllowed, null, res,
        200, log);
}
