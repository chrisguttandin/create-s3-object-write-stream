'use strict';

var _ = require('lodash'),
    EventEmitter = require('events').EventEmitter;

function S3MultipartUploader(s3Client, params) {
    this._emitter = new EventEmitter();
    this._isAborted = false;
    this._params = _.clone(params);
    this._parts = [];
    this._s3Client = s3Client;

    this._create();
}

S3MultipartUploader.prototype.abort = function () {
    var abort = this.abort.bind(this);

    this._isAborted = true;

    if (this._params.UploadId !== undefined) {
        this._s3Client.abortMultipartUpload(this._params, function (err) {
            if (err !== null) {
                if (err.code === 'NoSuchUpload') {
                    abort();
                } else {
                    throw err;
                }
            }
        });
    } else {
        this._emitter.on('created', abort);
    }
};

S3MultipartUploader.prototype._create = function () {
    var _onCreate = this._onCreate.bind(this);

    this._s3Client.createMultipartUpload(this._params, function (err, data) {
        if (err === null) {
            _onCreate(data.UploadId);
        } else {
            throw err;
        }
    });
};

S3MultipartUploader.prototype.complete = function (callback) {
    this._onCompleteCallback = callback;

    if (this._isWaitingForUploads()) {
        this._complete();
    }
};

S3MultipartUploader.prototype._complete = function () {
    var _complete = this._complete.bind(this),
        _onComplete,
        params;

    if (this._params.UploadId !== undefined) {
        _onComplete = this._onComplete.bind(this);

        params = _.merge({
            MultipartUpload: {
                Parts: this._parts.map(function (part) {
                    return {
                        ETag: part.eTag,
                        PartNumber: part.number.toString()
                    };
                })
            }
        }, this._params);

        this._s3Client.completeMultipartUpload(params, function (err) {
            if (err === null) {
                _onComplete();
            } else if (err.code === 'NoSuchUpload') {
                _complete();
            } else {
                throw err;
            }
        });
    } else {
        this._emitter.on('created', _complete);
    }
};

S3MultipartUploader.prototype._isWaitingForUploads = function () {
    return this._parts.some(function (part) {
        return part.eTag !== undefined;
    });
};

S3MultipartUploader.prototype._onComplete = function () {
    this._onCompleteCallback.call(null);
};

S3MultipartUploader.prototype._onCreate = function (uploadId) {
    this._params.UploadId = uploadId;
    this._emitter.emit('created');
};

S3MultipartUploader.prototype._onUpload = function (part, eTag) {
    part.eTag = eTag;

    if (this._onCompleteCallback !== undefined && this._isWaitingForUploads()) {
        this._complete();
    }
};

S3MultipartUploader.prototype.upload = function (buffer, part) {
    var _onUpload,
        params,
        upload = this.upload.bind(this);

    if (arguments.length === 1) {
        part = {
            number: this._parts.length + 1
        };

        this._parts.push(part);
    }

    if (this._params.UploadId !== undefined) {
        _onUpload = this._onUpload.bind(this);

        params = _.merge({
            Body: buffer,
            PartNumber: part.number.toString()
        }, this._params);

        this._s3Client.uploadPart(params, function (err, data) {
            if (err === null) {
                _onUpload(part, data.ETag);
            } else if (err.code === 'NoSuchUpload') {
                upload(buffer, part);
            } else {
                throw err;
            }
        });
    } else {
        this._emitter.on('created', this.upload.bind(this, buffer, part));
    }
};

module.exports.S3MultipartUploader = S3MultipartUploader;
