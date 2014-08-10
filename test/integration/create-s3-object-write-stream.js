'use strict';

var BUCKET = 'create-s3-object-write-stream-test',
    createS3ObjectWriteStream = require('../../src/create-s3-object-write-stream.js'),
    S3 = require('aws-sdk').S3;

describe('createS3ObjectWriteStream()', function () {

    var config,
        s3Client;

    afterEach(function (done) {
        s3Client.deleteBucket({
            Bucket: BUCKET
        }, done);
    });

    beforeEach(function () {
        try {
            config = require('../../config/credentials/aws.json');
        } catch (err) {
            throw new Error('Please create a file called "aws.json" inside the folder called "config/credentials". Look at the file called "config/credentials/aws-example.json" to see an example.');
        }
    });

    beforeEach(function (done) {
        s3Client = new S3(config);

        s3Client.createBucket({
            Bucket: BUCKET
        }, done);
    });

    it('should upload a small file', function (done) {
        var writeStream = createS3ObjectWriteStream(s3Client, {
                Bucket: BUCKET,
                Key: 'a-small-text-file.txt'
            });

        writeStream
            .on('finish', function () {
                s3Client.getObject({
                    Bucket: BUCKET,
                    Key: 'a-small-text-file.txt'
                }, function (err, data) {
                    expect(err).to.be.null;

                    expect(data.Body.toString()).to.equal('a small text');

                    s3Client.deleteObject({
                        Bucket: BUCKET,
                        Key: 'a-small-text-file.txt'
                    }, done);
                });
            })
            .end('a small text');
    });

    it('should upload a file larger than five megabytes', function (done) {
        var buffer = new Buffer(6291456), // 6 MB
            writeStream = createS3ObjectWriteStream(s3Client, {
                Bucket: BUCKET,
                Key: 'a-large-binary-file'
            });

        writeStream
            .on('finish', function () {
                s3Client.getObject({
                    Bucket: BUCKET,
                    Key: 'a-large-binary-file'
                }, function (err, data) {
                    expect(err).to.be.null;

                    expect(data.Body.length).to.equal(6291456);

                    s3Client.deleteObject({
                        Bucket: BUCKET,
                        Key: 'a-large-binary-file'
                    }, done);
                });
            });

        writeStream.write(buffer.slice(0, 1048576));
        writeStream.write(buffer.slice(1048576, 2097152));
        writeStream.write(buffer.slice(2097152, 3145728));
        writeStream.write(buffer.slice(3145728, 4194304));
        writeStream.write(buffer.slice(4194304, 5242880));
        writeStream.end(buffer.slice(5242880));
    });

});
