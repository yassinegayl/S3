import assert from 'assert';
import { errors } from 'arsenal';

import bucketPut from '../../../lib/api/bucketPut';
import bucketPutTagging from '../../../lib/api/bucketPutTagging';
import { _validator,
    parseTagXml } from '../../../lib/api/apiUtils/bucket/bucketTag';
import { cleanup,
    DummyRequestLogger,
    makeAuthInfo,
    TaggingConfigTester } from '../helpers';
import metadata from '../../../lib/metadata/wrapper';
import { taggingTests } from
  '../../functional/aws-node-sdk/lib/utility/tagging.js';

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketname';
const testBucketPutRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
};

function _checkError(err, code, errorName) {
    assert(err, 'Expected error but found none');
    assert.strictEqual(err.code, code);
    assert(err[errorName]);
}

function _generateSampleXml(key, value) {
    const xml = '<Tagging>' +
      '<TagSet>' +
         '<Tag>' +
           `<Key>${key}</Key>` +
           `<Value>${value}</Value>` +
         '</Tag>' +
      '</TagSet>' +
    '</Tagging>';

    return xml;
}

describe('putBucketTagging API', () => {
    beforeEach(done => {
        cleanup();
        bucketPut(authInfo, testBucketPutRequest, log, done);
    });
    afterEach(() => cleanup());

    it('should update a bucket\'s metadata with tags resource', done => {
        const taggingUtil = new TaggingConfigTester();
        const testBucketPutTaggingRequest = taggingUtil
            .createBucketTaggingRequest('PUT', bucketName);
        bucketPutTagging(authInfo, testBucketPutTaggingRequest, log, err => {
            if (err) {
                process.stdout.write(`Err putting website config ${err}`);
                return done(err);
            }
            return metadata.getBucket(bucketName, log, (err, bucket) => {
                if (err) {
                    process.stdout.write(`Err retrieving bucket MD ${err}`);
                    return done(err);
                }
                const uploadedTags = bucket.getTags();
                assert.deepStrictEqual(uploadedTags, taggingUtil.getTags());
                return done();
            });
        });
    });

    it('should return BadDigest if md5 is omitted', done => {
        const taggingUtil = new TaggingConfigTester();
        const testBucketPutTaggingRequest = taggingUtil
            .createBucketTaggingRequest('PUT', bucketName);
        testBucketPutTaggingRequest.headers['content-md5'] = undefined;
        bucketPutTagging(authInfo, testBucketPutTaggingRequest, log, err => {
            assert(err, 'Expected err but found none');
            assert.deepStrictEqual(err, errors.BadDigest);
            done();
        });
    });
});

describe('PUT bucket tagging :: helper validation functions ', () => {
    describe('validateTagStructure ', () => {
        it('should return expected true if tag is valid false/undefined if not',
        done => {
            const tags = [
                { tagTest: { Key: ['foo'], Value: ['bar'] }, isValid: true },
                { tagTest: { Key: ['foo'] }, isValid: false },
                { tagTest: { Value: ['bar'] }, isValid: false },
                { tagTest: { Keys: ['foo'], Value: ['bar'] }, isValid: false },
                { tagTest: { Key: ['foo', 'boo'], Value: ['bar'] },
                  isValid: false },
                { tagTest: { Key: ['foo'], Value: ['bar', 'boo'] },
                  isValid: false },
                { tagTest: { Key: ['foo', 'boo'], Value: ['bar', 'boo'] },
                  isValid: false },
                { tagTest: { Key: ['foo'], Values: ['bar'] }, isValid: false },
                { tagTest: { Keys: ['foo'], Values: ['bar'] }, isValid: false },
            ];

            for (let i = 0; i < tags.length; i++) {
                const tag = tags[i];
                const result = _validator.validateTagStructure(tag.tagTest);
                if (tag.isValid) {
                    assert(result);
                } else {
                    assert(!result);
                }
            }
            done();
        });

        describe('validateXMLStructure ', () => {
            it('should return expected true if tag is valid false/undefined ' +
            'if not', done => {
                const tags = [
                    { tagging: { Tagging: { TagSet: [{ Tag: [] }] } }, isValid:
                    true },
                    { tagging: { Tagging: { TagSet: [''] } }, isValid: true },
                    { tagging: { Tagging: { TagSet: [] } }, isValid: false },
                    { tagging: { Tagging: { TagSet: [{}] } }, isValid: false },
                    { tagging: { Tagging: { Tagset: [{ Tag: [] }] } }, isValid:
                    false },
                    { tagging: { Tagging: { Tagset: [{ Tag: [] }] },
                    ExtraTagging: 'extratagging' }, isValid: false },
                    { tagging: { Tagging: { Tagset: [{ Tag: [] }], ExtraTagset:
                    'extratagset' } }, isValid: false },
                    { tagging: { Tagging: { Tagset: [{ Tag: [] }], ExtraTagset:
                    'extratagset' } }, isValid: false },
                    { tagging: { Tagging: { Tagset: [{ Tag: [], ExtraTag:
                    'extratag' }] } }, isValid: false },
                    { tagging: { Tagging: { Tagset: [{ Tag: {} }] } }, isValid:
                    false },
                ];

                for (let i = 0; i < tags.length; i++) {
                    const tag = tags[i];
                    const result = _validator.validateXMLStructure(tag.tagging);
                    if (tag.isValid) {
                        assert(result);
                    } else {
                        assert(!result);
                    }
                }
                done();
            });
        });
    });

    describe('parseTagXml', () => {
        it('should parse a correct xml', done => {
            const xml = _generateSampleXml('foo', 'bar');
            parseTagXml(xml, log, (err, result) => {
                assert.strictEqual(err, null, `Found unexpected err ${err}`);
                assert.deepStrictEqual(result, [{ key: 'foo', value: 'bar' }]);
                return done();
            });
        });

        taggingTests.forEach(taggingTest => {
            it(taggingTest.it, done => {
                const xml = _generateSampleXml(taggingTest.tag.key,
                  taggingTest.tag.value);
                parseTagXml(xml, log, (err, result) => {
                    if (taggingTest.error) {
                        _checkError(err, 400, taggingTest.error);
                    } else {
                        assert.ifError(err, `Found unexpected err ${err}`);
                        assert.deepStrictEqual(result[0], taggingTest.tag);
                    }
                    return done();
                });
            });
        });
    });
});
