import assert from 'assert';
import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

const bucket = 'buckettestmultiplebackendget';
const memObject = 'memobject';
const fileObject = 'fileobject';
const awsObject = 'awsobject';

describe('Multiple backend get object', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        before(() => {
            process.stdout.write('Creating bucket');
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucketAsync({ Bucket: bucket })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        after(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil.empty(bucket)
            .then(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(bucket);
            })
            .catch(err => {
                process.stdout.write('Error emptying/deleting bucket: ' +
                `${err}\n`);
                throw err;
            });
        });

        it('should return an error to get request without a valid bucket name',
            done => {
                s3.getObject({ Bucket: '', Key: 'somekey' }, err => {
                    assert.notEqual(err, null,
                        'Expected failure but got success');
                    assert.strictEqual(err.code, 'MethodNotAllowed');
                    done();
                });
            });
        it('should return NoSuchKey error when no such object',
            done => {
                s3.getObject({ Bucket: bucket, Key: 'nope' }, err => {
                    assert.notEqual(err, null,
                        'Expected failure but got success');
                    assert.strictEqual(err.code, 'NoSuchKey');
                    done();
                });
            });

        describe('with objects in all available backends ' +
            '(mem/file/AWS)', () => {
            before(() => {
                process.stdout.write('Putting object to mem');
                return s3.putObjectAsync({ Bucket: bucket, Key: memObject,
                    Metadata: { 'scal-location-constraint': 'mem' } })
                .then(() => {
                    process.stdout.write('Putting object to file');
                    return s3.putObjectAsync({ Bucket: bucket, Key: fileObject,
                        Metadata: { 'scal-location-constraint': 'file' } });
                })
                .then(() => {
                    process.stdout.write('Putting object to AWS');
                    return s3.putObjectAsync({ Bucket: bucket, Key: awsObject,
                        Metadata: { 'scal-location-constraint': 'test-region' },
                    });
                })
                .catch(err => {
                    process.stdout.write(`Error putting objects: ${err}\n`);
                    throw err;
                });
            });
            it('should get an object from mem', done => {
                s3.getObject({ Bucket: bucket, Key: memObject }, err => {
                    assert.equal(err, null, 'Expected success but got error ' +
                        `${err}`);
                });
                done();
            });
            it('should get an object from file', done => {
                s3.getObject({ Bucket: bucket, Key: fileObject }, err => {
                    assert.equal(err, null, 'Expected success but got error ' +
                        `${err}`);
                });
                done();
            });
            it('should get an object from AWS', done => {
                s3.getObject({ Bucket: bucket, Key: awsObject }, err => {
                    assert.equal(err, null, 'Expected success but got error ' +
                        `${err}`);
                });
                done();
            });
        });
    });
});
