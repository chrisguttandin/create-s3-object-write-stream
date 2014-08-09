'use strict';

var Buffer = require('buffer').Buffer,
    MIN_PART_SIZE = 5242880, // 5 MB
    S3MultipartUploader = require('./s3-multipart-uploader.js').S3MultipartUploader,
    util = require('util'),
    Writable = require('stream').Writable;

function S3ObjectWriteStream(s3Client, params) {
    Writable.call(this);

    this._buffer = new Buffer(0);
    this._params = params;
    this._s3Client = s3Client;
    this._s3MultipartUploader = null;
}

util.inherits(S3ObjectWriteStream, Writable);

S3ObjectWriteStream.prototype.abort = function () {
    if (this._s3MultipartUploader !== null) {
        this._s3MultipartUploader.abort();
    }
};

S3ObjectWriteStream.prototype.emit = function (event) {
    if (event === 'finish') {
        this._finish(Writable.prototype.emit.bind(this, 'finish'));
        return this.listeners('finish').length > 0;
    } else {
        return Writable.prototype.emit.apply(Writable.prototype, arguments);
    }
};

S3ObjectWriteStream.prototype._finish = function (callback) {
    if (this._s3MultipartUploader === null) {
        this._uploadObject(callback);
    } else {
        if (this._buffer.length > 0) {
            this._uploadPart();
        }

        this._s3MultipartUploader.complete(callback);
    }
};

S3ObjectWriteStream.prototype._uploadObject = function (callback) {
    this._s3Client.putObject(_.merge({
        Body: this.buffer
    }, this._params), function(err, data) {
        if (err === null) {
            callback();
        }
    });
};

S3ObjectWriteStream.prototype._uploadPart = function () {
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
        if (this._s3MultipartUploader === null) {
            this._s3MultipartUploader = new S3MultipartUploader(this._s3Client, this._params);
        }

        this._uploadPart();
    }

    callback(null);
};

module.exports.S3ObjectWriteStream = S3ObjectWriteStream;
