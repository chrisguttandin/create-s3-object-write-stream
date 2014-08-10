'use strict';

var EventEmitter = require('events').EventEmitter,
    MAX_NUMBER_OF_RETRIES = 10,
    util = require('util');

function S3MultipartUploader(s3Client, params) {
    EventEmitter.call(this);

    this._emitter = new EventEmitter();
    this._isAborted = false;
    this._params = params;
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
        _fail,
        _onAbort = this._onAbort.bind(this),
        params;

    if (this._params.UploadId !== undefined) {
        _fail = this._fail.bind(this);

        params = {
            UploadId: this._params.UploadId
        };

        if (this._params.Bucket !== undefined) {
            params.Bucket = this._params.Bucket;
        }

        if (this._params.Key !== undefined) {
            params.Key = this._params.Key;
        }

        this._s3Client.abortMultipartUpload(params, function (err) {
            if (err === null) {
                _onAbort();
            } else if (attempt < MAX_NUMBER_OF_RETRIES && err.code === 'NoSuchUpload') {
                _abort(attempt + 1);
            } else {
                _fail(err);
            }
        });
    } else {
        this._emitter.on('created', _abort.bind(null, attempt));
    }
};

S3MultipartUploader.prototype._create = function () {
    var _fail,
        _onCreate;

    if (this._isAborted) {
        return;
    }

    _fail = this._fail.bind(this);
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
    var _complete,
        _fail,
        _onComplete,
        params;

    if (this._isAborted) {
        return;
    }

    _complete = this._complete.bind(this);

    if (this._params.UploadId !== undefined) {
        _fail = this._fail.bind(this);
        _onComplete = this._onComplete.bind(this);

        params = {
            MultipartUpload: {
                Parts: this._parts.map(function (part) {
                    return {
                        ETag: part.eTag,
                        PartNumber: part.number
                    };
                })
            },
            UploadId: this._params.UploadId
        };

        if (this._params.Bucket !== undefined) {
            params.Bucket = this._params.Bucket;
        }

        if (this._params.Key !== undefined) {
            params.Key = this._params.Key;
        }

        this._s3Client.completeMultipartUpload(params, function (err) {
            if (err === null) {
                _onComplete();
            } else if (attempt < MAX_NUMBER_OF_RETRIES && err.code === 'NoSuchUpload') {
                _complete(attempt + 1);
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

S3MultipartUploader.prototype._onAbort = function () {
    this.emit('aborted');
};

S3MultipartUploader.prototype._onComplete = function () {
    this._onCompleteCallback.call(null);
};

S3MultipartUploader.prototype._onCreate = function (uploadId) {
    this._verify(uploadId, 1);
};

S3MultipartUploader.prototype._onUpload = function (part, eTag) {
    part.eTag = eTag;

    if (this._onCompleteCallback !== undefined && !this._isWaitingForUploads()) {
        this._complete(1);
    }
};

S3MultipartUploader.prototype._onVerify = function (uploadId) {
    this._params.UploadId = uploadId;
    this._emitter.emit('created');
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
        _upload;

    if (this._isAborted) {
        return;
    }

    _upload = this._upload.bind(this, buffer, part);

    if (this._params.UploadId !== undefined) {
        _fail = this._fail.bind(this);
        _onUpload = this._onUpload.bind(this);

        params = {
            Body: buffer,
            PartNumber: part.number,
            UploadId: this._params.UploadId
        };

        if (this._params.Bucket !== undefined) {
            params.Bucket = this._params.Bucket;
        }

        if (this._params.Key !== undefined) {
            params.Key = this._params.Key;
        }

        this._s3Client.uploadPart(params, function (err, data) {
            if (err === null) {
                _onUpload(part, data.ETag);
            } else if (attempt < MAX_NUMBER_OF_RETRIES && err.code === 'NoSuchUpload') {
                _upload(attempt + 1);
            } else {
                _fail(err);
            }
        });
    } else {
        this._emitter.on('created', _upload.bind(null, attempt));
    }
};

S3MultipartUploader.prototype._verify = function (uploadId, attempt) {
    var _create = this._create.bind(this),
        _fail = this._fail.bind(this),
        _onVerify = this._onVerify.bind(this, uploadId),
        params = {
            UploadId: uploadId
        },
        _verify = this._verify.bind(this, uploadId, attempt + 1);

    if (this._params.Bucket !== undefined) {
        params.Bucket = this._params.Bucket;
    }

    if (this._params.Key !== undefined) {
        params.Key = this._params.Key;
    }

    this._s3Client.listParts(params, function (err, data) {
        if (err === null) {
            _onVerify();
        } else if (err.code === 'NoSuchUpload') {
            if (attempt < MAX_NUMBER_OF_RETRIES) {
                _verify();
            } else {
                _create();
            }
        } else {
            _fail(err);
        }
    });
};

module.exports.S3MultipartUploader = S3MultipartUploader;
