import assert from 'assert';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

const bucketName = 'testtaggingbucket';
import { taggingTests } from '../../lib/utility/tagging';

function generateMultipleTagConfig(nbr) {
    const tags = [];
    for (let i = 0; i < nbr; i++) {
        tags.push({ Key: `myKey${i}`, Value: `myValue${i}` });
    }
    return {
        TagSet: tags,
    };
}
function generateTaggingConfig(key, value) {
    return {
        TagSet: [
            {
                Key: key,
                Value: value,
            },
        ],
    };
}

function _checkError(err, code, statusCode) {
    assert(err, 'Expected error but found none');
    assert.strictEqual(err.code, code);
    assert.strictEqual(err.statusCode, statusCode);
}

describe('PUT bucket taggings', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        beforeEach(done => s3.createBucket({ Bucket: bucketName }, done));

        afterEach(() => bucketUtil.deleteOne(bucketName));

        taggingTests.forEach(taggingTest => {
            it(taggingTest.it, done => {
                const taggingConfig = generateTaggingConfig(taggingTest.tag.key,
                  taggingTest.tag.value);
                s3.putBucketTagging({ Bucket: bucketName,
                  Tagging: taggingConfig }, err => {
                    if (taggingTest.error) {
                        _checkError(err, taggingTest.error, 400);
                    } else {
                        assert.ifError(err, `Found unexpected err ${err}`);
                    }
                    done();
                });
            });
        });

        it('should return BadRequest if putting more that 50 tags', done => {
            const taggingConfig = generateMultipleTagConfig(51);
            s3.putBucketTagging({ Bucket: bucketName,
              Tagging: taggingConfig }, err => {
                _checkError(err, 'BadRequest', 400);
                done();
            });
        });

        it('should return InvalidTag if using the same key twice', done => {
            s3.putBucketTagging({ Bucket: bucketName,
              Tagging: { TagSet: [
                  {
                      Key: 'key1',
                      Value: 'value1',
                  },
                  {
                      Key: 'key1',
                      Value: 'value2',
                  },
              ] },
          }, err => {
                _checkError(err, 'InvalidTag', 400);
                done();
            });
        });
    });
});
