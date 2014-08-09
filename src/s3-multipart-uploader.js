'use strict';

var _ = require('lodash'),
    EventEmitter = require('events').EventEmitter,
    MAX_NUMBER_OF_RETRIES = 3,
    util = require('util');

function S3MultipartUploader(s3Client, params) {
    EventEmitter.call(this);

    this._emitter = new EventEmitter();
    this._isAborted = false;
    this._params = _.clone(params);
    this._parts = [];
    this._s3Client = s3Client;

    this._create();
}

util.inherits(S3MultipartUploader, EventEmitter);

S3MultipartUploader.prototype.abort = function () {
    this._isAborted = true;
    this._abort(1);
};

S3MultipartUploader.prototype._abort = function (attempt) {
    var _abort = this._abort.bind(this),
        _fail;

    if (this._params.UploadId !== undefined) {
        _fail = this._fail.bind(this);

        this._s3Client.abortMultipartUpload(this._params, function (err) {
            if (err !== null) {
                if (attempt < MAX_NUMBER_OF_RETRIES && err.code === 'NoSuchUpload') {
                    setTimeout(_abort.bind(null, attempt + 1), attempt * 100);
                } else {
                    _fail(err);
                }
            }
        });
    } else {
        this._emitter.on('created', _abort.bind(null, attempt));
    }
};

S3MultipartUploader.prototype._create = function () {
    var _fail = this._fail.bind(this),
        _onCreate = this._onCreate.bind(this);

    this._s3Client.createMultipartUpload(this._params, function (err, data) {
        if (err === null) {
            _onCreate(data.UploadId);
        } else {
            _fail(err);
        }
    });
};

S3MultipartUploader.prototype.complete = function (callback) {
    this._onCompleteCallback = callback;

    if (!this._isWaitingForUploads()) {
        this._complete(1);
    }
};

S3MultipartUploader.prototype._complete = function (attempt) {
    var _complete = this._complete.bind(this),
        _fail,
        _onComplete,
        params;

    if (this._params.UploadId !== undefined) {
        _fail = this._fail.bind(this);
        _onComplete = this._onComplete.bind(this);

        params = _.merge({
            MultipartUpload: {
                Parts: this._parts.map(function (part) {
                    return {
                        ETag: part.eTag,
                        PartNumber: part.number
                    };
                })
            }
        }, this._params);

        this._s3Client.completeMultipartUpload(params, function (err) {
            if (err === null) {
                _onComplete();
            } else if (attempt < MAX_NUMBER_OF_RETRIES && err.code === 'NoSuchUpload') {
                setTimeout(_complete.bind(null, attempt + 1), attempt * 100);
            } else {
                _fail(err);
            }
        });
    } else {
        this._emitter.on('created', _complete.bind(null, attempt));
    }
};

S3MultipartUploader.prototype._fail = function (err) {
    this.emit('error', err);
};

S3MultipartUploader.prototype._isWaitingForUploads = function () {
    return this._parts.some(function (part) {
        return part.eTag === undefined;
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

    if (this._onCompleteCallback !== undefined && !this._isWaitingForUploads()) {
        this._complete(1);
    }
};

S3MultipartUploader.prototype.upload = function (buffer) {
    var part = {
            number: this._parts.length + 1
        };

    this._parts.push(part);
    this._upload(buffer, part, 1);
};

S3MultipartUploader.prototype._upload = function (buffer, part, attempt) {
    var _fail,
        _onUpload,
        params,
        _upload = this._upload.bind(this, buffer, part);

    if (this._params.UploadId !== undefined) {
        _fail = this._fail.bind(this);
        _onUpload = this._onUpload.bind(this);

        params = _.merge({
            Body: buffer,
            PartNumber: part.number
        }, this._params);

        this._s3Client.uploadPart(params, function (err, data) {
            if (err === null) {
                _onUpload(part, data.ETag);
            } else if (attempt < MAX_NUMBER_OF_RETRIES && err.code === 'NoSuchUpload') {
                setTimeout(_upload.bind(null, attempt + 1), attempt * 100);
            } else {
                _fail(err);
            }
        });
    } else {
        this._emitter.on('created', _upload.bind(null, attempt));
    }
};

module.exports.S3MultipartUploader = S3MultipartUploader;
