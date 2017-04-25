import crypto from 'crypto';

import async from 'async';
import { errors } from 'arsenal';

import { metadataValidateBucket } from '../metadata/metadataUtils';
import { pushMetric } from '../utapi/utilities';
import collectCorsHeaders from '../utilities/collectCorsHeaders';
import metadata from '../metadata/wrapper';
import { parseTagXml } from './apiUtils/bucket/bucketTag';

/**
 * Bucket Put Tagging - Adds tag(s) to bucket
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
export default function bucketPutTagging(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketPutTagging' });

    if (!request.post) {
        log.debug('xml body is missing',
        { error: errors.MissingRequestBodyError });
        return callback(errors.MissingRequestBodyError);
    }

    const md5 = crypto.createHash('md5')
        .update(request.post, 'utf8').digest('base64');
    if (md5 !== request.headers['content-md5']) {
        log.debug('bad md5 digest', { error: errors.BadDigest });
        return callback(errors.BadDigest);
    }
    const bucketName = request.bucketName;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketOwnerAction',
    };

    return async.waterfall([
        next => metadataValidateBucket(metadataValParams, log,
          (err, bucket) => next(err, bucket)),
        (bucket, next) => {
            log.trace('parsing tag(s)');
            return parseTagXml(request.post, log, (err, tags) =>
              next(err, bucket, tags));
        },
        (bucket, tags, next) => {
            bucket.setTags(tags);
            metadata.updateBucket(bucket.getName(), bucket, log, err =>
                next(err, bucket));
        },
    ], (err, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.trace('error processing request', { error: err,
                method: 'bucketPutTagging' });
        } else {
            pushMetric('putBucketTagging', log, {
                authInfo,
                bucket: bucketName,
            });
        }
        return callback(err, corsHeaders);
    });
}
