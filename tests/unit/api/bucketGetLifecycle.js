const assert = require('assert');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketGetLifecycle = require('../../../lib/api/bucketGetLifecycle');
const bucketPutLifecycle = require('../../../lib/api/bucketPutLifecycle');
const { cleanup,
    DummyRequestLogger,
    makeAuthInfo }
    = require('../helpers');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketname';
const testBucketPutRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
};

function _makeLifecycleRequest(xml) {
    const request = {
        bucketName,
        headers: {
            host: `${bucketName}.s3.amazonaws.com`,
        },
    };
    if (xml) {
        request.post = xml;
    }
    return request;
}

function _makeLifecycleXml() {
    const id1 = 'test-id1';
    const id2 = 'test-id2';
    const prefix = 'test-prefix';
    const tags = [
        {
            key: 'test-key1',
            value: 'test-value1',
        },
    ];
    const action1 = 'Expiration';
    const days1 = 365;
    const action2 = 'NoncurrentVersionExpiration';
    const days2 = 1;
    return '<LifecycleConfiguration ' +
        'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
        '<Rule>' +
        `<ID>${id1}</ID>` +
        '<Status>Enabled</Status>' +
        `<Filter><Prefix>${prefix}</Prefix></Filter>` +
        `<${action1}><Days>${days1}</Days></${action1}>` +
        '</Rule>' +
        '<Rule>' +
        `<ID>${id2}</ID>` +
        '<Status>Enabled</Status>' +
        '<Filter><And>' +
        `<Prefix>${prefix}</Prefix>` +
        `<Tag><Key>${tags[0].key}</Key>` +
        `<Value>${tags[0].value}</Value></Tag>` +
        '</And></Filter>' +
        `<${action2}><NoncurrentDays>${days2}</NoncurrentDays></${action2}>` +
        '</Rule>' +
        '</LifecycleConfiguration>';
}

describe.only('getBucketLifecycle API', () => {
    before(() => cleanup());
    beforeEach(done => bucketPut(authInfo, testBucketPutRequest, log, done));
    afterEach(() => cleanup());

    it('should return NoSuchLifecycleConfiguration error if ' +
    'bucket has no lifecycle', done => {
        const lifecycleRequest = _makeLifecycleRequest();
        bucketGetLifecycle(authInfo, lifecycleRequest, log, err => {
            assert.strictEqual(err.NoSuchLifecycleConfiguration, true);
            done();
        });
    });

    describe('after bucket lifecycle has been put', () => {
        beforeEach(done => {
            const putRequest = _makeLifecycleRequest(_makeLifecycleXml());
            bucketPutLifecycle(authInfo, putRequest, log, err => {
                assert.equal(err, null);
                done();
            });
        });

        it('should return lifecycle XML', done => {
            const getRequest = _makeLifecycleRequest();
            bucketGetLifecycle(authInfo, getRequest, log, (err, res) => {
                assert.equal(err, null);
                const expectedXML = '<?xml version="1.0" encoding="UTF-8"?>' +
                    `${_makeLifecycleXml()}`;
                assert.deepStrictEqual(expectedXML, res);
                done();
            });
        });
    });
});
