'use strict';

function S3ClientStub() {
    this.abortMultipartUpload = sinon.stub();
    this.createMultipartUpload = sinon.stub();
    this.completeMultipartUpload = sinon.stub();
    this.uploadPart = sinon.stub();
}

module.exports.S3ClientStub = S3ClientStub;
