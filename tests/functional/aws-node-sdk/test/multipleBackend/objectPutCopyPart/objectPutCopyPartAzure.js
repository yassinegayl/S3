const async = require('async');
const assert = require('assert');

const { config } = require('../../../../../../lib/Config');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { uniqName } = require('../utils');

const describeSkipIfNotMultiple = (config.backends.data !== 'multiple'
    || process.env.S3_END_TO_END) ? describe.skip : describe;

const azureLocation = 'azuretest';
let azureContainerName;

if (config.locationConstraints[azureLocation] &&
config.locationConstraints[azureLocation].details &&
config.locationConstraints[azureLocation].details.azureContainerName) {
    azureContainerName =
      config.locationConstraints[azureLocation].details.azureContainerName;
}

const normalBody = Buffer.from('I am a body', 'utf8');
const normalMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';

const keyObject = 'objectputcopypartAzure';

let s3;
let bucketUtil;

describeSkipIfNotMultiple('Put Copy Part to AZURE', () => {
    withV4(sigCfg => {
        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil.empty(azureContainerName)
            .then(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(azureContainerName);
            })
            .catch(err => {
                process.stdout.write(`Error in afterEach: ${err}\n`);
                throw err;
            });
        });
        describe('Basic test: ', () => {
            beforeEach(function beF(done) {
                this.currentTest.keyName = uniqName(keyObject);
                this.currentTest.mpuKeyName = uniqName(keyObject);
                const params = {
                    Bucket: azureContainerName,
                    Key: this.currentTest.keyName,
                    Metadata: { 'scal-location-constraint': azureLocation },
                };
                async.waterfall([
                    next => s3.createBucket({ Bucket: azureContainerName },
                      err => next(err)),
                    next => s3.putObject({
                        Bucket: azureContainerName,
                        Key: this.currentTest.keyName,
                        Body: normalBody,
                        Metadata: { 'scal-location-constraint': azureLocation },
                    }, err => next(err)),
                    next => s3.createMultipartUpload(params, (err, res) => {
                        this.currentTest.uploadId = res.UploadId;
                        next(err);
                    }),
                ], done);
            });
            afterEach(function afterEachF(done) {
                const params = {
                    Bucket: azureContainerName,
                    Key: this.currentTest.keyName,
                    UploadId: this.currentTest.uploadId,
                };
                s3.abortMultipartUpload(params, done);
            });
            it('should copy part from Azure to Azure', function ifF(done) {
                const params = {
                    Bucket: azureContainerName,
                    CopySource:
                      `${azureContainerName}/${this.currentTest.keyName}`,
                    Key: this.currentTest.mpuKeyName,
                    PartNumber: 1,
                    UploadId: this.test.uploadId,
                };
                async.waterfall([
                    next => s3.uploadPartCopy(params, (err, res) => {
                        console.log('res!!!', res);
                        next(err);
                    }),
                ], done);
            });
        });
    });
});
