const async = require('async');
const assert = require('assert');
const { s3middleware } = require('arsenal');
const azureMpuUtils = s3middleware.azureHelper.mpuUtils;

const { config } = require('../../../../../../lib/Config');
const authdata = require('../../../../../../conf/authdata.json');
const constants = require('../../../../../../constants');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { uniqName, getAzureClient } = require('../utils');

const describeSkipIfNotMultiple = (config.backends.data !== 'multiple'
    || process.env.S3_END_TO_END) ? describe.skip : describe;

const azureLocation = 'azuretest';
const memLocation = 'mem';
let azureContainerName;

if (config.locationConstraints[azureLocation] &&
config.locationConstraints[azureLocation].details &&
config.locationConstraints[azureLocation].details.azureContainerName) {
    azureContainerName =
      config.locationConstraints[azureLocation].details.azureContainerName;
}

const normalBody = Buffer.from('I am a body', 'utf8');
const normalMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';

const oneKbBody = Buffer.alloc(1024);
const oneKbMD5 = '0f343b0931126a20f133d67c2b018a3b';

const tenMbBody = Buffer.alloc(1 * 1024 * 1024);
const tenMbMD5 = '0f343b0931126a20f133d67c2b018a3b';

const keyObjectAzure = 'objectputcopypartAzure';
const keyObjectMemory = 'objectputcopypartMemory';
const azureClient = getAzureClient();

const accountName = authdata.accounts[0].name;
const accountID = authdata.accounts[0].canonicalID;

const result = {
    Bucket: '',
    Key: '',
    UploadId: '',
    MaxParts: 1000,
    IsTruncated: false,
    Parts: [],
    Initiator:
     { ID: accountID,
       DisplayName: accountName },
    Owner:
     { DisplayName: accountName,
       ID: accountID },
    StorageClass: 'STANDARD',
};

let s3;
let bucketUtil;

describeSkipIfNotMultiple('Put Copy Part to AZURE', function describeF() {
    this.timeout(800000);
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
                this.currentTest.resultCopy =
                  JSON.parse(JSON.stringify(result));
                this.currentTest.keyNameNormalAzure = uniqName(keyObjectAzure);
                this.currentTest.keyNameTenMbAzure = uniqName(keyObjectAzure);
                this.currentTest.keyNameTenMbMem = uniqName(keyObjectMemory);
                this.currentTest.mpuKeyNameAzure = uniqName(keyObjectAzure);
                const params = {
                    Bucket: azureContainerName,
                    Key: this.currentTest.mpuKeyNameAzure,
                    Metadata: { 'scal-location-constraint': azureLocation },
                };
                async.waterfall([
                    next => s3.createBucket({ Bucket: azureContainerName },
                      err => next(err)),
                    next => s3.putObject({
                        Bucket: azureContainerName,
                        Key: this.currentTest.keyNameNormalAzure,
                        Body: normalBody,
                        Metadata: { 'scal-location-constraint': azureLocation },
                    }, err => next(err)),
                    next => s3.putObject({
                        Bucket: azureContainerName,
                        Key: this.currentTest.keyNameTenMbAzure,
                        Body: tenMbBody,
                        Metadata: { 'scal-location-constraint': azureLocation },
                    }, err => next(err)),
                    next => s3.putObject({
                        Bucket: azureContainerName,
                        Key: this.currentTest.keyNameTenMbMem,
                        Body: tenMbBody,
                        Metadata: { 'scal-location-constraint': memLocation },
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
                    Key: this.currentTest.mpuKeyNameAzure,
                    UploadId: this.currentTest.uploadId,
                };
                s3.abortMultipartUpload(params, done);
            });
            it('should copy small part from Azure to Azure',
            function ifF(done) {
                const params = {
                    Bucket: azureContainerName,
                    CopySource:
                      `${azureContainerName}/${this.test.keyNameNormalAzure}`,
                    Key: this.test.mpuKeyNameAzure,
                    PartNumber: 1,
                    UploadId: this.test.uploadId,
                };
                async.waterfall([
                    next => s3.uploadPartCopy(params, (err, res) => {
                        assert.equal(err, null, 'uploadPartCopy: Expected ' +
                        `success, got error: ${err}`);
                        assert.strictEqual(res.ETag, `"${normalMD5}"`);
                        next(err);
                    }),
                    next => s3.listParts({
                        Bucket: azureContainerName,
                        Key: this.test.mpuKeyNameAzure,
                        UploadId: this.test.uploadId,
                    }, (err, res) => {
                        assert.equal(err, null, 'listParts: Expected success,' +
                        ` got error: ${err}`);
                        this.test.resultCopy.Bucket = azureContainerName;
                        this.test.resultCopy.Key = this.test.mpuKeyNameAzure;
                        this.test.resultCopy.UploadId = this.test.uploadId;
                        this.test.resultCopy.Parts =
                         [{ PartNumber: 1,
                             LastModified: res.Parts[0].LastModified,
                             ETag: `"${normalMD5}"`,
                             Size: 11 }];
                        assert.deepStrictEqual(res, this.test.resultCopy);
                        next();
                    }),
                    next => azureClient.listBlocks(azureContainerName,
                    this.test.mpuKeyNameAzure, 'all', (err, res) => {
                        assert.equal(err, null, 'listBlocks: Expected ' +
                        `success, got error: ${err}`);
                        const partName = azureMpuUtils.getBlockId(
                          this.test.uploadId, 1, 0);
                        assert.strictEqual(res.UncommittedBlocks[0].Name,
                          partName);
                        assert.equal(res.UncommittedBlocks[0].Size, 11);
                        next();
                    }),
                ], done);
            });

            it('should copy 10 Mb part from Azure to Azure',
            function ifF(done) {
                const params = {
                    Bucket: azureContainerName,
                    CopySource:
                      `${azureContainerName}/${this.test.keyNameTenMbAzure}`,
                    Key: this.test.mpuKeyNameAzure,
                    PartNumber: 1,
                    UploadId: this.test.uploadId,
                };
                async.waterfall([
                    next => s3.uploadPartCopy(params, (err, res) => {
                        assert.equal(err, null, 'uploadPartCopy: Expected ' +
                        `success, got error: ${err}`);
                        assert.strictEqual(res.ETag, `"${normalMD5}"`);
                        next(err);
                    }),
                    next => s3.listParts({
                        Bucket: azureContainerName,
                        Key: this.test.mpuKeyNameAzure,
                        UploadId: this.test.uploadId,
                    }, (err, res) => {
                        assert.equal(err, null, 'listParts: Expected success,' +
                        ` got error: ${err}`);
                        this.test.resultCopy.Bucket = azureContainerName;
                        this.test.resultCopy.Key = this.test.mpuKeyNameAzure;
                        this.test.resultCopy.UploadId = this.test.uploadId;
                        this.test.resultCopy.Parts =
                         [{ PartNumber: 1,
                             LastModified: res.Parts[0].LastModified,
                             ETag: `"${normalMD5}"`,
                             Size: 11 }];
                        assert.deepStrictEqual(res, this.test.resultCopy);
                        next();
                    }),
                    next => azureClient.listBlocks(azureContainerName,
                    this.test.mpuKeyNameAzure, 'all', (err, res) => {
                        assert.equal(err, null, 'listBlocks: Expected ' +
                        `success, got error: ${err}`);
                        const partName = azureMpuUtils.getBlockId(
                          this.test.uploadId, 1, 0);
                        assert.strictEqual(res.UncommittedBlocks[0].Name,
                          partName);
                        assert.equal(res.UncommittedBlocks[0].Size, 11);
                        next();
                    }),
                ], done);
            });

            it.only('should copy 10 Mb part from a memory location to Azure',
            function ifF(done) {
                const params = {
                    Bucket: azureContainerName,
                    CopySource:
                      `${azureContainerName}/${this.test.keyNameTenMbMem}`,
                    Key: this.test.mpuKeyNameAzure,
                    PartNumber: 1,
                    UploadId: this.test.uploadId,
                };
                async.waterfall([
                    next => s3.uploadPartCopy(params, (err, res) => {
                        assert.equal(err, null, 'uploadPartCopy: Expected ' +
                        `success, got error: ${err}`);
                        assert.strictEqual(res.ETag, `"${normalMD5}"`);
                        next(err);
                    }),
                    next => s3.listParts({
                        Bucket: azureContainerName,
                        Key: this.test.mpuKeyNameAzure,
                        UploadId: this.test.uploadId,
                    }, (err, res) => {
                        assert.equal(err, null, 'listParts: Expected success,' +
                        ` got error: ${err}`);
                        this.test.resultCopy.Bucket = azureContainerName;
                        this.test.resultCopy.Key = this.test.mpuKeyNameAzure;
                        this.test.resultCopy.UploadId = this.test.uploadId;
                        this.test.resultCopy.Parts =
                         [{ PartNumber: 1,
                             LastModified: res.Parts[0].LastModified,
                             ETag: `"${normalMD5}"`,
                             Size: 11 }];
                        assert.deepStrictEqual(res, this.test.resultCopy);
                        next();
                    }),
                    next => azureClient.listBlocks(azureContainerName,
                    this.test.mpuKeyNameAzure, 'all', (err, res) => {
                        assert.equal(err, null, 'listBlocks: Expected ' +
                        `success, got error: ${err}`);
                        const partName = azureMpuUtils.getBlockId(
                          this.test.uploadId, 1, 0);
                        assert.strictEqual(res.UncommittedBlocks[0].Name,
                          partName);
                        assert.equal(res.UncommittedBlocks[0].Size, 11);
                        next();
                    }),
                ], done);
            });

            describe('with existing part', () => {
                beforeEach(function beF(done) {
                    const params = {
                        Body: oneKbBody,
                        Bucket: azureContainerName,
                        Key: this.currentTest.mpuKeyNameAzure,
                        PartNumber: 1,
                        UploadId: this.currentTest.uploadId,
                    };
                    s3.uploadPart(params, done);
                });
                it('should copy part from Azure to Azure with existing ' +
                'parts', function ifF(done) {
                    const params = {
                        Bucket: azureContainerName,
                        CopySource:
                        `${azureContainerName}/${this.test.keyNameNormalAzure}`,
                        Key: this.test.mpuKeyNameAzure,
                        PartNumber: 2,
                        UploadId: this.test.uploadId,
                    };
                    async.waterfall([
                        next => s3.uploadPartCopy(params, (err, res) => {
                            assert.equal(err, null,
                              'uploadPartCopy: Expected success, got ' +
                              `error: ${err}`);
                            assert.strictEqual(res.ETag, `"${normalMD5}"`);
                            next(err);
                        }),
                        next => s3.listParts({
                            Bucket: azureContainerName,
                            Key: this.test.mpuKeyNameAzure,
                            UploadId: this.test.uploadId,
                        }, (err, res) => {
                            assert.equal(err, null, 'listParts: Expected ' +
                            `success, got error: ${err}`);
                            this.test.resultCopy.Bucket = azureContainerName;
                            this.test.resultCopy.Key =
                            this.test.mpuKeyNameAzure;
                            this.test.resultCopy.UploadId = this.test.uploadId;
                            this.test.resultCopy.Parts =
                             [{ PartNumber: 1,
                                 LastModified: res.Parts[0].LastModified,
                                 ETag: `"${oneKbMD5}"`,
                                 Size: 1000 },
                               { PartNumber: 2,
                                   LastModified: res.Parts[1].LastModified,
                                   ETag: `"${normalMD5}"`,
                                   Size: 11 },
                             ];
                            assert.deepStrictEqual(res, this.test.resultCopy);
                            next();
                        }),
                        next => azureClient.listBlocks(azureContainerName,
                        this.test.mpuKeyNameAzure, 'all', (err, res) => {
                            assert.equal(err, null, 'listBlocks: Expected ' +
                            `success, got error: ${err}`);
                            const partName = azureMpuUtils.getBlockId(
                              this.test.uploadId, 1, 0);
                            const partName2 = azureMpuUtils.getBlockId(
                              this.test.uploadId, 2, 0);
                            assert.strictEqual(res.UncommittedBlocks[0].Name,
                              partName);
                            assert.equal(res.UncommittedBlocks[0].Size,
                            1024);
                            assert.strictEqual(res.UncommittedBlocks[1].Name,
                                partName2);
                            assert.equal(res.UncommittedBlocks[1].Size,
                            11);
                            next();
                        }),
                    ], done);
                });
            });
        });
    });
});
