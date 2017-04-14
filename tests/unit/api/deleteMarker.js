import { versioning } from 'arsenal';

import assert from 'assert';

import bucketPut from '../../../lib/api/bucketPut';
import bucketPutVersioning from '../../../lib/api/bucketPutVersioning';
import objectDelete from '../../../lib/api/objectDelete';
import metadata from '../metadataswitch';
import DummyRequest from '../DummyRequest';
import { cleanup, DummyRequestLogger, makeAuthInfo } from '../helpers';

const versionIdUtils = versioning.VersionID;

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const objectName = 'objectName';

const testPutBucketRequest = new DummyRequest({
    bucketName,
    namespace,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
});
const testDeleteRequest = new DummyRequest({
    bucketName,
    namespace,
    objectKey: objectName,
    headers: {},
    url: `/${bucketName}/${objectName}`,
});

function _createBucketPutVersioningReq(status) {
    const request = {
        bucketName,
        headers: {
            host: `${bucketName}.s3.amazonaws.com`,
        },
        url: '/?versioning',
        query: { versioning: '' },
    };
    const xml = '<VersioningConfiguration ' +
    'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
    `<Status>${status}</Status>` +
    '</VersioningConfiguration>';
    request.post = xml;
    return request;
}

const enableVersioningRequest = _createBucketPutVersioningReq('Enabled');

const expectedAcl = {
    Canned: 'private',
    FULL_CONTROL: [],
    WRITE_ACP: [],
    READ: [],
    READ_ACP: [],
};

const undefHeadersExpected = [
    'cache-control',
    'content-disposition',
    'content-encoding',
    'expires',
];

describe('delete marker creation', () => {
    beforeEach(done => {
        cleanup();
        bucketPut(authInfo, testPutBucketRequest, log, err => {
            if (err) {
                return done(err);
            }
            return bucketPutVersioning(authInfo, enableVersioningRequest,
                log, done);
        });
    });

    afterEach(() => {
        cleanup();
    });

    function _assertDeleteMakerMd(deleteMarkerMD, deleteResHeaderVersionId) {
        const mdVersionId = deleteMarkerMD.versionId;
        assert.strictEqual(deleteMarkerMD.isDeleteMarker, true);
        assert.strictEqual(versionIdUtils.encode(mdVersionId),
            deleteResHeaderVersionId);
        assert.strictEqual(deleteMarkerMD['content-length'], 0);
        assert.strictEqual(deleteMarkerMD.location, null);
        assert.deepStrictEqual(deleteMarkerMD.acl, expectedAcl);
        undefHeadersExpected.forEach(header => {
            assert.strictEqual(deleteMarkerMD[header], undefined);
        });
    }

    it('should create a delete marker if versioning enabled and deleting ' +
    'object without version id', done => {
        objectDelete(authInfo, testDeleteRequest, log, (err, delResHeaders) => {
            if (err) {
                return done(err);
            }
            assert.strictEqual(delResHeaders['x-amz-delete-marker'], true);
            assert(delResHeaders['x-amz-version-id']);
            return metadata.getObjectMD(bucketName, objectName, {}, log,
                (err, deleteMarkerMD) => {
                    _assertDeleteMakerMd(deleteMarkerMD,
                        delResHeaders['x-amz-version-id']);
                    return done();
                });
        });
    });
});
