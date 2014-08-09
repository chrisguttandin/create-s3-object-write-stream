'use strict';

var Buffer = require('buffer').Buffer,
    S3MultipartUploaderStub = require('../stubs/s3-multipart-uploader.js').S3MultipartUploaderStub,
    s3ObjectWriteStreamModule = rewire('../src/s3-object-write-stream.js');

describe('S3ObjectWriteStream', function () {

    var s3ObjectWriteStream;

    beforeEach(function () {
        s3ObjectWriteStreamModule.__set__('S3MultipartUploader', S3MultipartUploaderStub);

        s3ObjectWriteStream = new s3ObjectWriteStreamModule.S3ObjectWriteStream();
    });

    it('should upload chunks of a least 5 mega bytes', function () {
        var buffer = new Buffer(1048576); // 1 MB

        s3ObjectWriteStream.write(buffer);
        s3ObjectWriteStream.write(buffer);
        s3ObjectWriteStream.write(buffer);
        s3ObjectWriteStream.write(buffer);

        expect(s3ObjectWriteStream._s3MultipartUploader).to.be.null;

        s3ObjectWriteStream.write(buffer);

        expect(s3ObjectWriteStream._s3MultipartUploader.complete).to.have.not.been.called;
        expect(s3ObjectWriteStream._s3MultipartUploader.upload).to.have.been.calledOnce;

        s3ObjectWriteStream.end(buffer);

        expect(s3ObjectWriteStream._s3MultipartUploader.complete).to.have.been.calledOnce;
        expect(s3ObjectWriteStream._s3MultipartUploader.upload).to.have.been.calledTwice;
    });

});
