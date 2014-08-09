'use strict';

function S3MultipartUploaderStub() {
    this.complete = sinon.stub();
    this.on = sinon.stub();
    this.upload = sinon.stub();
}

module.exports.S3MultipartUploaderStub = S3MultipartUploaderStub;
