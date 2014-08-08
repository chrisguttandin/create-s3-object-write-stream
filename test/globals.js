'use strict';

var chai = require('chai'),
    sinonChai = require('sinon-chai');

chai.use(sinonChai);

global.expect = require('chai').expect;
global.sinon = require('sinon');
global.rewire = require('rewire');
