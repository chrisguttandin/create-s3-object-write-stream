'use strict';

var Buffer = require('buffer').Buffer,
    MIN_PART_SIZE = 5242880, // 5 MB
    S3MultipartUploader = require('./s3-multipart-uploader.js').S3MultipartUploader,
    util = require('util'),
    Writable = require('stream').Writable;

function S3ObjectWriteStream(s3Client, params) {
    Writable.call(this);

    this._buffer = new Buffer(0);
    this._s3MultipartUploader = new S3MultipartUploader(s3Client, params);
}

util.inherits(S3ObjectWriteStream, Writable);

S3ObjectWriteStream.prototype.abort = function () {
    return this._s3MultipartUploader.abort();
};

S3ObjectWriteStream.prototype.emit = function (event) {
    if (event === 'finish') {
        this._upload();
        this._s3MultipartUploader.complete(Writable.prototype.emit.bind(this, 'finish'));
        return this.listeners('finish').length > 0;
    } else {
        return Writable.prototype.emit.apply(Writable.prototype, arguments);
    }
};

S3ObjectWriteStream.prototype._upload = function () {
    this._s3MultipartUploader.upload(this._buffer);
    this._buffer = new Buffer(0);
};

S3ObjectWriteStream.prototype._write = function (chunk, encoding, callback) {
    var totalLength = this._buffer.length + chunk.length;

    this._buffer = Buffer.concat([
        this._buffer,
        chunk
    ], totalLength);

    if (totalLength >= MIN_PART_SIZE) {
        this._upload();
    }

    callback(null);
};

module.exports.S3ObjectWriteStream = S3ObjectWriteStream;
