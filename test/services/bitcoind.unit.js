'use strict';

/* jshint sub: true */

var path = require('path');
var EventEmitter = require('events').EventEmitter;
var should = require('chai').should();
var crypto = require('crypto');
var bitcore = require('bitcore-lib');
var _ = bitcore.deps._;
var sinon = require('sinon');
var proxyquire = require('proxyquire');
var fs = require('fs');
var sinon = require('sinon');

var index = require('../../lib');
var log = index.log;
var errors = index.errors;

var Transaction = bitcore.Transaction;
var readFileSync = sinon.stub().returns(fs.readFileSync(path.resolve(__dirname, '../data/bitcoin.conf')));
var BitcoinService = proxyquire('../../lib/services/bitcoind', {
  fs: {
    readFileSync: readFileSync
  }
});
var defaultBitcoinConf = fs.readFileSync(path.resolve(__dirname, '../data/default.bitcoin.conf'), 'utf8');

describe('Bitcoin Service', function() {
});
