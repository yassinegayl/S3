import assert from 'assert';
import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

const bucket = 'buckettestmultiplebackendput';

describe('MultipleBackend put object', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        beforeEach(() => {
            process.stdout.write('Creating bucket');
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucketAsync({ Bucket: bucket })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil.empty(bucket)
            .then(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(bucket);
            })
            .catch(err => {
                process.stdout.write(`Error in afterEach: ${err}\n`);
                throw err;
            });
        });

        it('should put an object to mem', done => {
            const params = { Bucket: bucket, Key: 'key',
                Metadata: { 'scal-location-constraint': 'mem' },
            };
            s3.putObject(params, err => {
                assert.equal(err, null, 'Expected success, ' +
                    `got error ${JSON.stringify(err)}`);
                done();
            });
        });
        it('should put an object to file', done => {
            const params = { Bucket: bucket, Key: 'key',
                Metadata: { 'scal-location-constraint': 'file' },
            };
            s3.putObject(params, err => {
                assert.equal(err, null, 'Expected success, ' +
                    `got error ${JSON.stringify(err)}`);
                done();
            });
        });
        it('should put an object to real AWS', done => {
            const params = { Bucket: bucket, Key: 'key',
                Metadata: { 'scal-location-constraint': 'aws-us-east-test' },
            };
            s3.putObject(params, err => {
                assert.equal(err, null, 'Expected success, ' +
                    `got error ${JSON.stringify(err)}`);
                done();
            });
        });
    });
});
