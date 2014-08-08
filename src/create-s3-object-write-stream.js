'use strict';

var S3ObjectWriteStream = require('./s3-object-write-stream.js').S3ObjectWriteStream;

module.exports = function createS3ObjectWriteStream(s3Client, params) {
    return new S3ObjectWriteStream(s3Client, params);
};
