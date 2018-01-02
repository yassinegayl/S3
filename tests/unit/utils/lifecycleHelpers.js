

function _getLifecycleRequest(bucketName, xml) {
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

function _getLifecycleXml() {
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
        `<Filter><Prefix>${prefix}</Prefix></Filter>` +
        '<Status>Enabled</Status>' +
        `<${action1}><Days>${days1}</Days></${action1}>` +
        '</Rule>' +
        '<Rule>' +
        `<ID>${id2}</ID>` +
        '<Filter><And>' +
        `<Prefix>${prefix}</Prefix>` +
        `<Tag><Key>${tags[0].key}</Key>` +
        `<Value>${tags[0].value}</Value></Tag>` +
        '</And></Filter>' +
        '<Status>Enabled</Status>' +
        `<${action2}><NoncurrentDays>${days2}</NoncurrentDays></${action2}>` +
        '</Rule>' +
        '</LifecycleConfiguration>';
}
