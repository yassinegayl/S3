const AWS = require('aws-sdk');
const fs = require('fs');
const path = require('path');
const { config } = require('../../../../../lib/Config');
const https = require('https');

function getAwsCredentials(profile, credFile) {
    const filename = path.join(process.env.HOME, credFile);

    try {
        fs.statSync(filename);
    } catch (e) {
        const msg = `AWS credential file does not exist: ${filename}`;
        throw new Error(msg);
    }

    return new AWS.SharedIniFileCredentials({ profile, filename });
}

function getRealAwsConfig(awsLocation) {
    const { credentialsProfile, credentials: locCredentials } =
        config.locationConstraints[awsLocation].details;
    const connectionAgent = new https.Agent({ keepAlive: true });
    if (credentialsProfile) {
        const credentials = getAwsCredentials(credentialsProfile,
            '/.aws/credentials');
        return {
            httpOptions: { agent: connectionAgent },
            credentials,
            signatureVersion: 'v4',
        };
    }
    return {
        httpOptions: { agent: connectionAgent },
        accessKeyId: locCredentials.accessKey,
        secretAccessKey: locCredentials.secretKey,
        signatureVersion: 'v4',
    };
}

module.exports = {
    getRealAwsConfig,
    getAwsCredentials,
};
