import { waterfall } from 'async';

import { metadataValidateBucket } from '../metadata/metadataUtils';
import getReplicationConfiguration from './apiUtils/bucket/bucketReplication';
import metadata from '../metadata/wrapper';
import collectCorsHeaders from '../utilities/collectCorsHeaders';

/**
 * bucketPutReplication - Create or update bucket replication configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
export default function bucketPutReplication(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketPutReplication' });
    const { bucketName, post, headers, method } = request;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketPutReplication',
    };
    return waterfall([
        next => getReplicationConfiguration(post, log, next),
        (config, next) => metadataValidateBucket(metadataValParams, log,
            (err, bucket) => next(err, config, bucket)),
        (config, bucket, next) => {
            bucket.setReplicationConfiguration(config);
            // TODO: all metadata updates of bucket should be using CAS.
            metadata.updateBucket(bucket.getName(), bucket, log, err =>
                next(err, bucket));
        },
    ], (err, bucket) => {
        const corsHeaders = collectCorsHeaders(headers.origin, method, bucket);
        if (err) {
            log.trace('error processing request', {
                error: err,
                method: 'bucketPutReplication',
            });
            return callback(err, corsHeaders);
        }
        // TODO: Push metrics for bucketPutReplication.
        // pushMetric('bucketPutReplication', log, {
        //     authInfo,
        //     bucket: bucketName,
        // }
        return callback(null, corsHeaders);
    });
}
