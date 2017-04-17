import { errors } from 'arsenal';
import assert from 'assert';

import bucketPut from '../../../lib/api/bucketPut';
import bucketPutReplication from '../../../lib/api/bucketPutReplication';
import { cleanup, DummyRequestLogger, makeAuthInfo } from '../helpers';

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const testBucketPutRequest = {
    bucketName,
    namespace,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
};

describe('putBucketReplication API', () => {
    before(() => cleanup());
    beforeEach(done => bucketPut(authInfo, testBucketPutRequest, log, done));
    afterEach(() => cleanup());

    it.skip('should return an error if xml provided does not contain Role',
    done => {
        const testReplicationRequest = {
            bucketName,
            namespace,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            post: '<ReplicationConfiguration xmlns=' +
                    '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                    '<Role>IAM-role-ARN</Role>' +
                    '<Rule>' +
                        '<ID>Rule-1</ID>' +
                        '<Status>rule-status</Status>' +
                        '<Prefix>key-prefix</Prefix>' +
                        '<Destination>' +
                            '<Bucket>arn:aws:s3:::bucket-name</Bucket>' +
                            '<StorageClass>' +
                                'optional-destination-storage-class-override' +
                            '</StorageClass>' +
                        '</Destination>' +
                    '</Rule>' +
                    '<Rule>' +
                        '<ID>Rule-2</ID>' +
                    '</Rule>' +
                '</ReplicationConfiguration>',
            url: '/?acl',
            query: { acl: '' },
        };

        bucketPutReplication(authInfo, testReplicationRequest, log, err => {
            assert.deepStrictEqual(err, errors.MalformedACLError);
            done();
        });
    });
});
