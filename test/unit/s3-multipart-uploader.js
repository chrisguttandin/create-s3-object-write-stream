'use strict';

var _ = require('lodash'),
    Buffer = require('buffer').Buffer,
    S3ClientStub = require('../stubs/s3-client.js').S3ClientStub,
    S3MultipartUploader = require('../../src/s3-multipart-uploader.js').S3MultipartUploader;

describe('S3MultipartUploader', function () {

    var eTag,
        params,
        s3Client,
        s3MultipartUploader,
        uploadId;

    beforeEach(function () {
        eTag = 'a-fake-e-tag';

        params = {
            Key: 'a-fake-key'
        };

        uploadId = 'a-fake-upload-id';

        s3Client = new S3ClientStub();
        s3Client.createMultipartUpload.yields(null, {
            UploadId: uploadId
        });
        s3Client.completeMultipartUpload.yields(null);
        s3Client.listParts.yields(null);
        s3Client.uploadPart.yields(null, {
            ETag: eTag
        });

        s3MultipartUploader = new S3MultipartUploader(s3Client, params);
    });

    it('should abort a multipart uploader right away', function (done) {
        s3MultipartUploader.abort();

        setTimeout(function () {
            expect(s3Client.abortMultipartUpload).to.have.been.calledOnce;
            expect(s3Client.abortMultipartUpload).to.have.been.calledWith(_.merge({
                UploadId: uploadId
            }, params));

            expect(s3Client.createMultipartUpload).to.have.been.calledOnce;
            expect(s3Client.createMultipartUpload).to.have.been.calledWith(_.merge({
                UploadId: uploadId
            }, params));

            expect(s3Client.completeMultipartUpload).to.have.not.been.called;

            expect(s3Client.listParts).to.have.been.calledOnce;
            expect(s3Client.listParts).to.have.been.calledWith(_.merge({
                UploadId: uploadId
            }, params));

            expect(s3Client.uploadPart).to.have.not.been.called;

            done();
        }, 100);
    });

    it('should abort a multipart uploader after uploading a buffer', function (done) {
        var buffer = new Buffer(5242880);

        s3MultipartUploader.upload(buffer);
        s3MultipartUploader.abort();

        setTimeout(function () {
            expect(s3Client.abortMultipartUpload).to.have.been.calledOnce;
            expect(s3Client.abortMultipartUpload).to.have.been.calledWith(_.merge({
                UploadId: uploadId
            }, params));

            expect(s3Client.createMultipartUpload).to.have.been.calledOnce;
            expect(s3Client.createMultipartUpload).to.have.been.calledWith(_.merge({
                UploadId: uploadId
            }, params));

            expect(s3Client.completeMultipartUpload).to.have.not.been.called;

            expect(s3Client.listParts).to.have.been.calledOnce;
            expect(s3Client.listParts).to.have.been.calledWith(_.merge({
                UploadId: uploadId
            }, params));

            expect(s3Client.uploadPart).to.have.been.calledOnce;
            expect(s3Client.uploadPart).to.have.been.calledWith(_.merge({
                Body: buffer,
                PartNumber: 1,
                UploadId: uploadId
            }, params));

            done();
        }, 100);
    });

    it('should complete a multipart uploader after uploading a buffer', function (done) {
        var buffer = new Buffer(5242880);

        s3MultipartUploader.upload(buffer);
        s3MultipartUploader.complete(function () {
            expect(s3Client.abortMultipartUpload).to.have.not.been.called;

            expect(s3Client.createMultipartUpload).to.have.been.calledOnce;
            expect(s3Client.createMultipartUpload).to.have.been.calledWith(_.merge({
                UploadId: uploadId
            }, params));

            expect(s3Client.completeMultipartUpload).to.have.been.calledOnce;
            expect(s3Client.completeMultipartUpload).to.have.been.calledWith(_.merge({
                MultipartUpload: {
                    Parts: [
                        {
                            ETag: eTag,
                            PartNumber: 1
                        }
                    ]
                },
                UploadId: uploadId
            }, params));

            expect(s3Client.listParts).to.have.been.calledOnce;
            expect(s3Client.listParts).to.have.been.calledWith(_.merge({
                UploadId: uploadId
            }, params));

            expect(s3Client.uploadPart).to.have.been.calledOnce;
            expect(s3Client.uploadPart).to.have.been.calledWith(_.merge({
                Body: buffer,
                PartNumber: 1,
                UploadId: uploadId
            }, params));

            done();
        });
    });

    it('should emit an error if multipart upload creation fails', function (done) {
        s3Client.createMultipartUpload.yieldsAsync('a fake error');

        s3MultipartUploader = new S3MultipartUploader(s3Client, params);

        s3MultipartUploader.on('error', function (err) {
            expect(err).to.equal('a fake error');

            done();
        });
    });

    it('should try to upload a buffer three times when uploads fail with an "NoSuchUpload" error', function (done) {
        var buffer = new Buffer(5242880);

        s3Client.uploadPart.yields({
            code: 'NoSuchUpload'
        });

        s3MultipartUploader.on('error', function (err) {
            expect(err.code).to.equal('NoSuchUpload');

            expect(s3Client.uploadPart.callCount).to.equal(10);
            expect(s3Client.uploadPart).to.have.been.calledWith(_.merge({
                Body: buffer,
                PartNumber: 1,
                UploadId: uploadId
            }, params));

            done();
        });

        s3MultipartUploader.upload(buffer);
    });

    it('should try to complete the multipart upload three times if it fails with an "NoSuchUpload" error', function (done) {
        var buffer = new Buffer(5242880);

        s3Client.completeMultipartUpload.yields({
            code: 'NoSuchUpload'
        });

        s3MultipartUploader.on('error', function (err) {
            expect(err.code).to.equal('NoSuchUpload');

            expect(s3Client.completeMultipartUpload.callCount).to.equal(10);
            expect(s3Client.completeMultipartUpload).to.have.been.calledWith(_.merge({
                MultipartUpload: {
                    Parts: []
                },
                UploadId: uploadId
            }, params));

            done();
        });

        s3MultipartUploader.complete(done);
    });

    it('should try to abort the multipart upload three times if it fails with an "NoSuchUpload" error', function (done) {
        var buffer = new Buffer(5242880);

        s3Client.abortMultipartUpload.yields({
            code: 'NoSuchUpload'
        });

        s3MultipartUploader.on('error', function (err) {
            expect(err.code).to.equal('NoSuchUpload');

            expect(s3Client.abortMultipartUpload.callCount).to.equal(10);
            expect(s3Client.abortMultipartUpload).to.have.been.calledWith(_.merge({
                UploadId: uploadId
            }, params));

            done();
        });

        s3MultipartUploader.abort();
    });

});
