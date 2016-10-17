'use strict';

var assert = require('assert');
var fs = require('fs');
var path = require('path');
var spawn = require('child_process').spawn;
var util = require('util');
var mkdirp = require('mkdirp');
var bitcore = require('bitcore-lib');
var async = require('async');
var LRU = require('lru-cache');
var BitcoinRPC = require('bitcoind-rpc');
var $ = bitcore.util.preconditions;
var _  = bitcore.deps._;
var Transaction = bitcore.Transaction;

var index = require('../');
var errors = index.errors;
var log = index.log;
var utils = require('../utils');
var Service = require('../service');
var Promise = require('bluebird');

var redis = require('redis');
Promise.promisifyAll(redis.RedisClient.prototype);
Promise.promisifyAll(redis.Multi.prototype);

process.on('unhandledRejection', function(reason, promise) {
  console.error('Unhandled rejection:');
  console.error(reason.stack);
  process.exit(1);
});

const createErrorWithCode = (message, code) => {
  const error = new Error(message);
  error.code = code;
  return error;
};

const createNotFoundError = (message) => createErrorWithCode(message, -5);

function Bitcoin(options) {
  if (!(this instanceof Bitcoin)) {
    return new Bitcoin(options);
  }

  Service.call(this, options);
  this.options = options;

  // event subscribers
  this.subscriptions = {};
  this.subscriptions.rawtransaction = [];
  this.subscriptions.hashblock = [];
  this.subscriptions.address = {};

  this.mempool = [];

  this.on('error', function(err) {
    log.error(err.stack);
  });
}

util.inherits(Bitcoin, Service);

Bitcoin.dependencies = [];

// Bitcoin.DEFAULT_MAX_TXIDS = 1000;
// Bitcoin.DEFAULT_MAX_HISTORY = 50;
// Bitcoin.DEFAULT_SHUTDOWN_TIMEOUT = 15000;
// Bitcoin.DEFAULT_ZMQ_SUBSCRIBE_PROGRESS = 0.9999;
// Bitcoin.DEFAULT_MAX_ADDRESSES_QUERY = 10000;
// Bitcoin.DEFAULT_SPAWN_RESTART_TIME = 5000;
// Bitcoin.DEFAULT_SPAWN_STOP_TIME = 10000;
// Bitcoin.DEFAULT_TRY_ALL_INTERVAL = 1000;
// Bitcoin.DEFAULT_REINDEX_INTERVAL = 10000;
// Bitcoin.DEFAULT_START_RETRY_INTERVAL = 5000;
// Bitcoin.DEFAULT_TIP_UPDATE_INTERVAL = 15000;
// Bitcoin.DEFAULT_TRANSACTION_CONCURRENCY = 5;
// Bitcoin.DEFAULT_CONFIG_SETTINGS = {
//   server: 1,
//   whitelist: '127.0.0.1',
//   txindex: 1,
//   addressindex: 1,
//   timestampindex: 1,
//   spentindex: 1,
//   zmqpubrawtx: 'tcp://127.0.0.1:28332',
//   zmqpubhashblock: 'tcp://127.0.0.1:28332',
//   rpcallowip: '127.0.0.1',
//   rpcuser: 'bitcoin',
//   rpcpassword: 'local321',
//   uacomment: 'bitcore'
// };

/**
 * Called by Node to determine the available API methods.
 */
Bitcoin.prototype.getAPIMethods = function() {
  var methods = [
    ['getBlock', this, this.getBlock, 1],
    ['getRawBlock', this, this.getRawBlock, 1],
    ['getBlockHeader', this, this.getBlockHeader, 1],
    ['getBlockOverview', this, this.getBlockOverview, 1],
    ['getBlockHashesByTimestamp', this, this.getBlockHashesByTimestamp, 2],
    ['getBestBlockHash', this, this.getBestBlockHash, 0],
    ['getSpentInfo', this, this.getSpentInfo, 1],
    ['getInfo', this, this.getInfo, 0],
    ['syncPercentage', this, this.syncPercentage, 0],
    ['isSynced', this, this.isSynced, 0],
    ['getRawTransaction', this, this.getRawTransaction, 1],
    ['getTransaction', this, this.getTransaction, 1],
    ['getDetailedTransaction', this, this.getDetailedTransaction, 1],
    ['sendTransaction', this, this.sendTransaction, 1],
    ['estimateFee', this, this.estimateFee, 1],
    ['getAddressTxids', this, this.getAddressTxids, 2],
    ['getAddressBalance', this, this.getAddressBalance, 2],
    ['getAddressUnspentOutputs', this, this.getAddressUnspentOutputs, 2],
    ['getAddressHistory', this, this.getAddressHistory, 2],
    ['getAddressSummary', this, this.getAddressSummary, 1],
    ['generateBlock', this, this.generateBlock, 1]
  ];
  return methods;
};

/**
 * Called by the Bus to determine the available events.
 */
Bitcoin.prototype.getPublishEvents = function() {
  return [
    {
      name: 'bitcoind/rawtransaction',
      scope: this,
      subscribe: this.subscribe.bind(this, 'rawtransaction'),
      unsubscribe: this.unsubscribe.bind(this, 'rawtransaction')
    },
    {
      name: 'bitcoind/hashblock',
      scope: this,
      subscribe: this.subscribe.bind(this, 'hashblock'),
      unsubscribe: this.unsubscribe.bind(this, 'hashblock')
    },
    {
      name: 'bitcoind/addresstxid',
      scope: this,
      subscribe: this.subscribeAddress.bind(this),
      unsubscribe: this.unsubscribeAddress.bind(this)
    }
  ];
};

Bitcoin.prototype.subscribe = function(name, emitter) {
  this.subscriptions[name].push(emitter);
  log.info(emitter.remoteAddress, 'subscribe:', 'bitcoind/' + name, 'total:', this.subscriptions[name].length);
};

Bitcoin.prototype.unsubscribe = function(name, emitter) {
  var index = this.subscriptions[name].indexOf(emitter);
  if (index > -1) {
    this.subscriptions[name].splice(index, 1);
  }
  log.info(emitter.remoteAddress, 'unsubscribe:', 'bitcoind/' + name, 'total:', this.subscriptions[name].length);
};

Bitcoin.prototype.subscribeAddress = function(emitter, addresses) {
  var self = this;

  function addAddress(addressStr) {
    if(self.subscriptions.address[addressStr]) {
      var emitters = self.subscriptions.address[addressStr];
      var index = emitters.indexOf(emitter);
      if (index === -1) {
        self.subscriptions.address[addressStr].push(emitter);
      }
    } else {
      self.subscriptions.address[addressStr] = [emitter];
    }
  }

  for(var i = 0; i < addresses.length; i++) {
    if (bitcore.Address.isValid(addresses[i], this.node.network)) {
      addAddress(addresses[i]);
    }
  }

  log.info(emitter.remoteAddress, 'subscribe:', 'bitcoind/addresstxid', 'total:', _.size(this.subscriptions.address));
};

Bitcoin.prototype.unsubscribeAddress = function(emitter, addresses) {
  var self = this;
  if(!addresses) {
    return this.unsubscribeAddressAll(emitter);
  }

  function removeAddress(addressStr) {
    var emitters = self.subscriptions.address[addressStr];
    var index = emitters.indexOf(emitter);
    if(index > -1) {
      emitters.splice(index, 1);
      if (emitters.length === 0) {
        delete self.subscriptions.address[addressStr];
      }
    }
  }

  for(var i = 0; i < addresses.length; i++) {
    if(this.subscriptions.address[addresses[i]]) {
      removeAddress(addresses[i]);
    }
  }

  log.info(emitter.remoteAddress, 'unsubscribe:', 'bitcoind/addresstxid', 'total:', _.size(this.subscriptions.address));
};

/**
 * A helper function for the `unsubscribe` method to unsubscribe from all addresses.
 * @param {String} name - The name of the event
 * @param {EventEmitter} emitter - An instance of an event emitter
 */
Bitcoin.prototype.unsubscribeAddressAll = function(emitter) {
  for(var hashHex in this.subscriptions.address) {
    var emitters = this.subscriptions.address[hashHex];
    var index = emitters.indexOf(emitter);
    if(index > -1) {
      emitters.splice(index, 1);
    }
    if (emitters.length === 0) {
      delete this.subscriptions.address[hashHex];
    }
  }
  log.info(emitter.remoteAddress, 'unsubscribe:', 'bitcoind/addresstxid', 'total:', _.size(this.subscriptions.address));
};

/**
 * Called by Node to start the service
 * @param {Function} callback
 */
Bitcoin.prototype.start = function(callback) {
  var config = this.options.connect[0];

  this.rpc = new BitcoinRPC({
    protocol: config.rpcprotocol || 'http',
    host: config.rpchost || '127.0.0.1',
    port: config.rpcport,
    user: config.rpcuser,
    pass: config.rpcpassword,
    rejectUnauthorized: _.isUndefined(config.rpcstrict) ? true : config.rpcstrict
  });

  if (process.env.REDIS_URL) {
    this.db = redis.createClient({ url: process.env.REDIS_URL });
  } else {
    this.db = redis.createClient();
  }

  log.info('loading tip');

  this.db.getAsync('height')
    .then(height => {
      log.info('loaded height', height);
      this.height = height;

      this._monitorBlocks();
      this._monitorMempool();

      callback();
    })
    .catch(callback);
};

const GENESIS_TX_HEX = '01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff5a04f0ff0f1e01044c4c54696d657320323031342f31302f3331204d61696e65204a756467652053617973204e75727365204d75737420466f6c6c6f772045626f6c612051756172616e74696e6520666f72204e6f7704823f0000ffffffff0100000000000000000000000000';
const GENESIS_TXID = '365d2aa75d061370c9aefdabac3985716b1e3b4bb7c4af4ed54f25e5aaa42783';

Bitcoin.prototype._fetchTransaction = function(txid) {
  if (txid === GENESIS_TXID) {
    return Promise.resolve({
      hex: GENESIS_TX_HEX,
      height: 0,
      vout: [],
      vin: [],
      version: 1,
      txid: txid,
    });
  }

  return this._rpcp('getRawTransaction', txid)
    .then(hex => this._rpcp('decodeRawTransaction', hex)
      .then(tx => Object.assign({ txid, hex }, tx)));
}

const toSatoshis = value => Math.round(value * 1e8);

const parseOutput = (vout, index) => {
  const address = vout.scriptPubKey &&
    vout.scriptPubKey.addresses &&
    vout.scriptPubKey.addresses.length === 1 &&
    vout.scriptPubKey.addresses[0];

  return Promise.resolve({
    satoshis: toSatoshis(vout.value),
    script: vout.scriptPubKey.hex,
    scriptAsm: vout.scriptPubKey.asm,
    address: address || null,
  });
};

Bitcoin.prototype._getInputValue = function(vin) {
  return Promise.resolve().then(() => {
    // TODO: Why is this needed?
    if (vin.coinbase) { return toSatoshis(50); }

    // TODO: Understand zero spends
    if (!+vin.txid) { return 0; }

    // Fetch the value from previous transaction output
    return this.db.getAsync(`tx:${vin.txid}`)
      .then(JSON.parse)
      .then(prevTx => {
        if (!prevTx) {
          throw Error(`Prev tx ${vin.txid} referenced by ${txid}:${vin.vout} not found`);
        }

        return prevTx.outputs[vin.vout].valueSat;
      });
    });
};

Bitcoin.prototype._parseInput = function(txIsCoinbase, vin, index) {
  return this._getInputValue(vin)
     .then(valueSat => ({
       prevTxId: vin.txid || null,
       outputIndex: _.isUndefined(vin.vout) ? null : vin.vout,
       sequence: vin.sequence,
       address: vin.address || null,
       satoshis: _.isUndefined(valueSat) ? null : valueSat,
       script: vin.scriptSig ? vin.scriptSig.hex : vin.coinbase ? vin.coinbase : null,
       scriptAsm: vin.scriptSig ? vin.scriptSig.asm : null,
     }));
};

Bitcoin.prototype._parseOutput = function(vout, index) {
  const address = vout.scriptPubKey &&
    vout.scriptPubKey.addresses &&
    vout.scriptPubKey.addresses.length === 1 &&
    vout.scriptPubKey.addresses[0];

  return Promise.resolve({
    satoshis: toSatoshis(vout.value),
    script: vout.scriptPubKey.hex,
    scriptAsm: vout.scriptPubKey.asm,
    address: address || null,
  });
};

// Rpc tx to inernal tx
Bitcoin.prototype._parseBlockRpcTx = function(block, rpcTx) {
  assert(rpcTx.txid, 'txid missing from rpcTx');
  const isCoinbase = !!(rpcTx.vin[0] && rpcTx.vin[0].coinbase);

  return Promise.all([
    Promise.map(rpcTx.vin, this._parseInput.bind(this, isCoinbase)),
    Promise.map(rpcTx.vout, this._parseOutput),
  ])
    .then(([inputs, outputs]) => {
      return Promise.resolve({
        hex: rpcTx.hex,
        blockHash: block ? block.hash : null,
        height: block ? block.height : null,
        blockTimestamp: rpcTx.time || (block ? block.time : null),
        version: rpcTx.version,
        hash: rpcTx.txid,
        locktime: rpcTx.locktime,
        inputs,
        outputs,
        coinbase: isCoinbase,
        inputSatoshis: inputs.reduce((sum, input) => sum + input.satoshis, 0),
        outputSatoshis: outputs.reduce((sum, output) => sum + output.satoshis, 0),
      });
    });
};

Bitcoin.prototype._scanBlockTx = function(block, txid) {
  log.info(`scanning tx ${txid}`);

  const storeAddressesFromInputs = (vins) => {
    return Promise.all(vins.map(vin => {
      if (!vin.address) { return; }
      return this.db.saddAsync(`address:${vin.address}:txids`, txid);
    }));
  };

  const storeAddressesFromOutputs = (vouts) => {
    return Promise.all(vouts.map(vout => {
      if (!vout.scriptPubKey) { return; }
      const { addresses } = vout.scriptPubKey;
      if (!addresses) { return; }

      return Promise.all(addresses.map(address =>
        this.db.saddAsync(`address:${address}:txids`, txid)));
    }));
  };

  const storeSentFromInputs = (inputs) => {
    return Promise.all(inputs.map((input, index) => {
      if (!input.address) { return; }
      return Promise.all([
        this.db.incrbyAsync(`address:${input.address}:sent`, input.satoshis),
        this.db.srem(`address:${output.address}:utxo`, `${txid}:${index}`),
      ]);
    }));
  };

  const storeReceivedFromOutputs = (outputs) => {
    return Promise.all(outputs.map((output, index) => {
      if (!output.address) { return; }
      return Promise.all([
        this.db.incrbyAsync(`address:${output.address}:received`, output.satoshis),
        this.db.sadd(`address:${output.address}:utxo`, `${txid}:${index}`),
      ]);
    }));
  };

  return this._fetchTransaction(txid)
    .then(rpcTx => {
      return Promise.all([
        this._parseBlockRpcTx(block, rpcTx).then(tx => {
          return Promise.all([
            storeSentFromInputs(tx.inputs),
            storeReceivedFromOutputs(tx.outputs),
            this.db.setAsync(`tx:${txid}`, JSON.stringify(tx)),
          ]);
        }),
        storeAddressesFromInputs(rpcTx.vin),
        storeAddressesFromOutputs(rpcTx.vout),
      ]);
    });
}

Bitcoin.prototype._scanBlock = function(hash) {
  log.info(`scanning block ${hash}`);

  // TODO: hmset
  return Promise.resolve()
    .then(() => Promise.all([
      this._rpcp('getBlock', hash),
      this._rpcp('getBlock', hash, false),
    ]))
    .then(([block, hex]) => {
      return Promise.all([
        this.db.setAsync(`block:${hash}`, JSON.stringify(Object.assign({ hex }, block))),
        this.db.setAsync(`blockhash:${block.height}`, hash),
        this.db.setAsync('height', block.height),
        this.db.zadd('blocktimes', block.time, hash),
        Promise.each(block.tx, txid => this._scanBlockTx(block, txid)),
      ]);
    });
}

Bitcoin.prototype._monitorBlocks = function() {
  log.info('monitoring blocks');

  var scan = () => Promise.resolve()
    .then(() => this._rpcp('getBlockCount'))
    .then(rpcHeight => {
      if (rpcHeight === this.height) {
        if (!this.ready) {
          this.ready = true;
          this.emit('synced', this.height);
          this.emit('ready');
        }

        return;
      }

      log.info(`rpc height ${rpcHeight}`);

      if (rpcHeight < this.height) {
        throw new Error('Height reduced');
      }

      return Promise.each(_.range(this.height || 0, rpcHeight + 1), height => {
        return this._rpcp('getBlockHash', height)
          .then(hash => {
            // TODO: See if there's been a re-org?
            return this._scanBlock(hash)
              .then(() => {
                this.height = height;
                this.emit('block', hash);
                this.emit('tip', this.height);

                return this.db.getAsync(`block:${hash}`)
                  .then(block => {
                    // TODO: Not sure how to get block hex
                    this.subscriptions.hashblock.forEach(x => x.emit('bitcoind/hashblock', block.hex));
                  });
              });
          });
      })
        .then(() => {
          this.emit('synced', this.height);
        });
    })
    .then(() => {
      if (!this.stopping) { setTimeout(scan, 1e3); }
    });

  scan();

  // TODO: And scan again
}

Bitcoin.prototype._monitorMempool = function() {
  log.info('monitoring mempool');

  var scan = () => Promise.resolve()
    .then(() => this._rpcp('getRawMemPool'))
    .then(mempool => {
      const added = mempool.filter(txid => !~this.mempool.indexOf(txid));
      const removed = this.mempool.filter(txid => !~mempool.indexOf(txid));

      if (added.length) {
        log.info(`added to mempool: ${added.join(', ')}`);
      }

      if (removed.length) {
        log.info(`removed from mempool: ${removed.join(', ')}`);
      }

      this.mempool = mempool;

      return Promise.each(added, txid => {
        return this._fetchTransaction(txid)
          .then(tx => {
            return this.db.getAsync(`block:${tx.blockHash}`)
              .then(block => this._parseBlockRpcTx(block, tx))
          })
          .then(tx => {
            assert(tx.hex, 'hex missing from tx');

            const message = new Buffer(tx.hex, 'hex');

            // TODO: Format?
            this.emit('tx', message);

            // Notify transaction subscribers
            for (var i = 0; i < this.subscriptions.rawtransaction.length; i++) {
              this.subscriptions.rawtransaction[i].emit('bitcoind/rawtransaction', message.toString('hex'));
            }

            const addresses = _.uniq([...tx.inputs, ...tx.outputs]
              .reduce((prev, point) => point.address ? [...prev, point.address] : prev, []));

            addresses.forEach(address => {
              const emitters = this.subscriptions.address[address];
              if (!emitters) { return; }
              emitters.forEach(x => x.emit('bitcoind/addresstxid', {
                address: address,
                txid: txid,
              }));
            });

            this.mempool.push(txid);
          });
      });
    })
    .then(() => {
      if (!this.stopping) { setTimeout(scan, 1e3); }
    });

  scan();

  // TODO: And scan again
}

/**
 * Helper to determine the state of the database.
 * @param {Function} callback
 */
Bitcoin.prototype.isSynced = function(callback) {
  // TODO: How to know this?
  this.syncPercentage((err, pct) => {
    if (err) { return callback(err); }
    callback(null, pct === 100.00);
  });
};

/**
 * Helper to determine the progress of the database.
 * @param {Function} callback
 */
Bitcoin.prototype.syncPercentage = function(callback) {
  callback(null, 100.00);
};

/**
 * Will get the balance for an address or multiple addresses
 * @param {String|Address|Array} addressArg - An address string, bitcore address, or array of addresses
 * @param {Object} options
 * @param {Function} callback
 */
Bitcoin.prototype.getAddressBalance = function(addressArg, options, callback) {
  throw new Error('getAddressBalance not implemented');
};

/**
 * Will get the unspent outputs for an address or multiple addresses
 * @param {String|Address|Array} addressArg - An address string, bitcore address, or array of addresses
 * @param {Object} options
 * @param {Function} callback
 */
Bitcoin.prototype.getAddressUnspentOutputs = function(addressArg, options, callback) {
  if (typeof addressArg === 'string') {
    return this.getAddressUnspentOutputs([addressArg], options, callback);
  }

  assert(Array.isArray(addressArg));

  const forAddressUtxo = (address, txid, index) => {
    console.log('for address what', address, txid, index);
    return this.db.getAsync(`tx:${txid}`)
      .then(JSON.parse)
      .then(tx => {
        console.log('? le tx', tx);
        return {
          address,
          txid,
          outputIndex: index,
          script: tx.outputs[index].script,
          satoshis: tx.outputs[index].satoshis,
          height: tx.height,
        };
      });
  };

  const forAddress = address => {
    return this.db.smembersAsync(`address:${address}:utxo`)
      .then(utxos => {
        console.log('mapping what?', utxos);
        return Promise.map(utxos, utxoJoined => {
          console.log('???? even has a value?', utxoJoined);
          const [txid, indexAsStr] = utxoJoined.split(':');
          const index = +indexAsStr;
          return forAddressUtxo(address, txid, index);
        });
      });
    };

  console.log('??? mapping arr?', addressArg);

  return Promise.map(addressArg, forAddress)
    .then(result => {
      callback(null, _.flatten(result));
    });

  // TODO: This needs some redis love

  // ~$ bitcoin-cli getaddressutxos '{"addresses": ["12cbQLTFMXRnSzktFkuoG3eHoMeFtpTu3S"]}'
  // [
  //   {
  //     "address": "12cbQLTFMXRnSzktFkuoG3eHoMeFtpTu3S",
  //     "txid": "1554a02d4eb1c7a73e3736922ed99530e360784e709896c42e5756e65b2da341",
  //     "outputIndex": 2,
  //     "script": "76a91411b366edfc0a8b66feebae5c2e25a7b6a5d1cf3188ac",
  //     "satoshis": 1,
  //     "height": 220151
  //   },
  //   {
  //     "address": "12cbQLTFMXRnSzktFkuoG3eHoMeFtpTu3S",
  //     "txid": "20fb69a94413637cb50f65e473f91d2599a04d5a0bf9bf6a5e9e843df2710ea4",
  //     "outputIndex": 0,
  //     "script": "76a91411b366edfc0a8b66feebae5c2e25a7b6a5d1cf3188ac",
  //     "satoshis": 30000,
  //     "height": 228208
  //   },
  //   ...
  // ]};
}

/**
 * Will get the txids for an address or multiple addresses
 * @param {String|Address|Array} addressArg - An address string, bitcore address, or array of addresses
 * @param {Object} options
 * @param {Function} callback
 */
Bitcoin.prototype.getAddressTxids = function(addressArg, options, callback) {
  throw new Error('getAddressTxids not implemented');
};

/**
 * Will detailed transaction history for an address or multiple addresses
 * @param {String|Address|Array} addressArg - An address string, bitcore address, or array of addresses
 * @param {Object} options
 * @param {Function} callback
 */
Bitcoin.prototype.getAddressHistory = function(addressArg, options, callback) {
  this.db.smembersAsync(`address:${addressArg}:txids`)
    .then(txids => {
      return Promise.map(txids, txid => {
        return this.db.getAsync(`tx:${txid}`)
          .then(JSON.parse)
          .then(tx => {
            return {
              tx: tx,
            };
          });
      })
        .then(txs => callback(null, {
          totalCount: txids.length,
          items: txs,
        }));
    })
    .catch(callback);

  // addresses: addressDetails.addresses,
  // satoshis: addressDetails.satoshis,
  // confirmations: self._getConfirmationsDetail(transaction),
  // tx: transaction
  //
  //
  // this.getAddressTxids(addresses, options, (err, txids) => {
  //   if (err) { return callback(err); }
  //
  //   var totalCount = txids.length;
  //
  //   // TODO: Paging
  //   // try {
  //   //   txids = self._paginateTxids(txids, fromArg, toArg);
  //   // } catch(e) {
  //   //   return callback(e);
  //   // }
  //
  //   async.mapLimit(
  //     txids,
  //     10, // TODO: Wut
  //     function(txid, next) {
  //       self._getAddressDetailedTransaction(txid, {
  //         queryMempool: queryMempool,
  //         addressStrings: addressStrings
  //       }, next);
  //     },
  //     function(err, transactions) {
  //       if (err) {
  //         return callback(err);
  //       }
  //       callback(null, {
  //         totalCount: totalCount,
  //         items: transactions
  //       });
  //     }
  //   );
  // });
};

/**
 * Will get the summary including txids and balance for an address or multiple addresses
 * @param {String|Address|Array} addressArg - An address string, bitcore address, or array of addresses
 * @param {Object} options
 * @param {Function} callback
 */
Bitcoin.prototype.getAddressSummary = function(addressArg, options, callback) {
  // TODO: This is a full scan. Need to cache in some other way?
  // TODO: Invalidation will be tough
  // TODO: Need to do one full sweep back and then avoid sweeping again
  // TODO: For higher blocks
  // TODO: queryMempool
  // TODO: from to
  // TODO: Race conditions ahead

  if (Array.isArray(addressArg)) {
    return async.map(addressArg, (addr, next) => {
      this.getAddressSummary(addr, options, next);
    }, (err, res) => {
      if (err) { return callback(err); }
      throw new Error('Not implemented');
    });
  }

  const address = addressArg;

  if (this.height === null) {
    return callback(new Error('Height must be known'));
  }

  return Promise.all([
    this.db.smembersAsync(`address:${address}:txids`),
  ])
    .then(([txids]) => {
      return callback(null, {
        totalReceived: 0, // TODO
        totalSpent: 0, // TODO
        balance: 0, // TODO
        unconfirmedAppearances: 0, // TODO
        unconfirmedBalance: 0, // TODO
        appearances: txids.length,
        txids, // TODO: Paging
      });
    });

  // options.queryMempool)
  // var fromArg = parseInt(options.from || 0);
  // var toArg = parseInt(options.to || self.maxTxids);
};

/**
 * Will retrieve a block as a Node.js Buffer
 * @param {String|Number} block - A block hash or block height number
 * @param {Function} callback
 */
Bitcoin.prototype.getRawBlock = function(block, callback) {
  if (typeof block === 'number') {
    return this.db.getAsync(`blockhash:${block}`)
      .then(hash => {
        if (!hash) { return callback(createNotFoundError('Block not found')); }
        return this.getRawBlock(hash, callback);
      });
  }

  // TODO: hmget
  this.db.getAsync(`block:${block}`)
    .then(JSON.parse)
    .then(block => {
      if (!block) { return callback(createNotFoundError('Block not found')); }
      callback(null, block.hex);
    })
    .catch(callback);
};

const blockHashOrNumberWrap = function(fn) {
  return function (block, ...rest) {
    const callback = rest[rest.length - 1];

    if (typeof block === 'number') {
      return this.db.getAsync(`blockhash:${block}`)
        .then(hash => {
          if (!hash) { return callback(createNotFoundError('Block not found')); }
          fn.call(this, hash, ...rest);
        });
    }

    fn.call(this, block, ...rest);
  };
}

/**
 * Similar to getBlockHeader but will include a list of txids
 * @param {String|Number} block - A block hash or block height number
 * @param {Function} callback
 */
Bitcoin.prototype.getBlockOverview = blockHashOrNumberWrap(function(block, callback) {
  log.info('fetching block overview');

  // TODO: hmget
  this.db.getAsync(`block:${block}`)
    .then(JSON.parse)
    .then(block => {
      if (!block) { return callback(createNotFoundError('Block not found')); }

      callback(null, {
        hash: block.hash,
        version: block.version,
        confirmations: block.confirmations,
        height: block.height,
        chainWork: block.chainwork,
        prevHash: block.previousblockhash,
        nextHash: block.nextblockhash,
        merkleRoot: block.merkleroot,
        time: block.time,
        medianTime: block.mediantime,
        nonce: block.nonce,
        bits: block.bits,
        difficulty: block.difficulty,
        txids: block.tx
      });
    })
    .catch(callback);
});

/**
 * Will retrieve a block as a Bitcore object
 * @param {String|Number} block - A block hash or block height number
 * @param {Function} callback
 */
Bitcoin.prototype.getBlock = function(block, callback) {
  if (typeof block === 'number') {
    return this.db.getAsync(`blockhash:${block}`)
      .then(hash => {
        if (!hash) { return callback(createNotFoundError('Block not found')); }
        return this.getBlock(hash, callback);
      });
  }

  // TODO: hmget
  this.db.getAsync(`block:${block}`)
    .then(JSON.parse)
    .then(block => {
      if (!block) { return callback(createNotFoundError('Block not found')); }
      callback(null, bitcore.Block.fromString(block.hex));
    })
    .catch(callback);
};

/**
 * Will retrieve an array of block hashes within a range of timestamps
 * @param {Number} high - The more recent timestamp in seconds
 * @param {Number} low - The older timestamp in seconds
 * @param {Function} callback
 */
Bitcoin.prototype.getBlockHashesByTimestamp = function(high, low, options, callback) {
  if (_.isFunction(options)) {
    callback = options;
    options = {};
  }

  this.db.zrangebyscoreAsync('blocktimes', low, high)
    .then(hashes => callback(null, hashes))
    .catch(callback);
};

Bitcoin.prototype.getBlockHeader = function(block, callback) {
  if (typeof block === 'number') {
    return this.db.getAsync(`blockhash:${block}`)
      .then(hash => {
        if (!hash) { return callback(createNotFoundError('Block not found')); }
        return this.getBlockHeader(hash, callback);
      });
  }

  // TODO: hmget
  this.db.getAsync(`block:${block}`)
    .then(JSON.parse)
    .then(block => {
      if (!block) { return callback(createNotFoundError('Block not found')); }

      callback(null, {
        hash: block.hash,
        version: block.version,
        confirmations: block.confirmations,
        height: block.height,
        chainWork: block.chainwork,
        prevHash: block.previousblockhash,
        nextHash: block.nextblockhash,
        merkleRoot: block.merkleroot,
        time: block.time,
        medianTime: block.time,
        nonce: block.nonce,
        bits: block.bits,
        difficulty: block.difficulty
      });
    })
    .catch(callback);
};

/**
 * Will estimate the fee per kilobyte.
 * @param {Number} blocks - The number of blocks for the transaction to be confirmed.
 * @param {Function} callback
 */
Bitcoin.prototype.estimateFee = function(blocks, callback) {
  callback(null, 0.00001);
};

/**
 * Will add a transaction to the mempool and relay to connected peers
 * @param {String|Transaction} transaction - The hex string of the transaction
 * @param {Object=} options
 * @param {Boolean=} options.allowAbsurdFees - Enable large fees
 * @param {Function} callback
 */
Bitcoin.prototype.sendTransaction = function(tx, options, callback) {
  if (_.isFunction(options) && _.isUndefined(callback)) {
    callback = options;
  } else if (_.isObject(options)) {
    // allowAbsurdFees = options.allowAbsurdFees;
  }

  this._rpcp('sendRawTransaction', tx)
    .then(result => {
      callback(null, result);
    })
    .catch(callback);
};

/**
 * Will get a transaction as a Node.js Buffer. Results include the mempool.
 * @param {String} txid - The transaction hash
 * @param {Function} callback
 */
Bitcoin.prototype.getRawTransaction = function(txid, callback) {
  throw new Error('getRawTransaction not implemented');
};

/**
 * Will get a transaction as a Bitcore Transaction. Results include the mempool.
 * @param {String} txid - The transaction hash
 * @param {Boolean} queryMempool - Include the mempool
 * @param {Function} callback
 */
Bitcoin.prototype.getTransaction = function(txid, callback) {
  this.db.getAsync(`tx:${txid}`)
    .then(JSON.parse)
    .then(tx => {
      if (!tx) { return callback(createNotFoundError('Transaction not found')); }
      callback(null, tx.txid);
    })
    .catch(callback);

    this.db.getAsync(`tx:${txid}`)
      .then(JSON.parse)
      .then(rpcTx => {
        if (!rpcTx) { return callback(createNotFoundError('Transaction not found')); }

        var tx = Transaction();
        assert(rpcTx.hex);

        tx.fromString(rpcTx.hex);

        callback(null, tx);
      })
      .catch(callback);
};

// Bitcoin.prototype._getOutputValueSat = function(txid, index, callback) {
//   assert.equal(typeof index, 'number');
//
//   this._fetchAndDecodeTx(LONG_CACHE, txid, (err, tx) => {
//     if (err) { return callback(err); }
//     callback(null, tx.vout[index].value * 1e8);
//   });
// }

Bitcoin.prototype.getDetailedTransaction = function(txid, callback) {
  this.db.getAsync(`tx:${txid}`)
    .then(JSON.parse)
    .then(tx => {
      if (!tx) console.log('!!!!!!!!!!!!!!!!!!! did not find it', txid);
      if (!tx) { return callback(createNotFoundError('Transaction not found')); }
      console.log('okay so whats the tx here?', tx);
      callback(null, tx);
    })
    .catch(callback);
};

/**
 * Will get the best block hash for the chain.
 * @param {Function} callback
 */
Bitcoin.prototype.getBestBlockHash = function(callback) {
  throw new Error('getBestBlockHash not implemented');
};

/**
 * Will give the txid and inputIndex that spent an output
 * @param {Function} callback
 */
Bitcoin.prototype.getSpentInfo = function(options, callback) {
  throw new Error('getSpentInfo not implemented');
};

/**
 * This will return information about the database in the format:
 * {
 *   version: 110000,
 *   protocolVersion: 70002,
 *   blocks: 151,
 *   timeOffset: 0,
 *   connections: 0,
 *   difficulty: 4.6565423739069247e-10,
 *   testnet: false,
 *   network: 'testnet'
 *   relayFee: 1000,
 *   errors: ''
 * }
 * @param {Function} callback
 */
Bitcoin.prototype.getInfo = function(callback) {
  this._rpcp('getInfo')
    .then(info => callback(null, {
      version: info.version,
      protocolVersion: info.protocolversion,
      blocks: info.blocks,
      timeOffset: info.timeoffset,
      connections: info.connections,
      proxy: info.proxy,
      difficulty: info.difficulty,
      testnet: info.testnet,
      relayFee: info.relayfee,
      errors: info.errors,
      network: this.options.connect[0].network,
    }));
};

Bitcoin.prototype._rpcp = function(...args) {
  return new Promise((resolve, reject) => {
    const method = args[0];
    const rest = args.slice(1);
    // log.info(`rpc -> ${method} ${rest.join(', ')}`);

    const callback = (err, response) => {
      if (err) { return reject(err); }
      if (response.error) { return reject(new Error(response.error)); }
      resolve(response.result);
    };

    const cacheKey = [method, args].join('/');

    const canUseCache = !~[
      'getBlockCount',
      'getInfo',
      'getRawMemPool',
    ].indexOf(method);

    return this.db.getAsync(`rpccache:${cacheKey}`)
      .then(cached => {
        if (canUseCache && cached) {
          // log.info('rpc cache HIT ' + cacheKey);
          return callback(null, JSON.parse(cached));
        }

        // log.info('rpc cache MISS')

        this.rpc[method].call(this.rpc, ...rest, (err, response) => {
          if (!err) {
            this.db.setAsync(`rpccache:${cacheKey}`, JSON.stringify(response)).catch(console.error);
          }
          callback(err, response);
        });
      });
  });
}

Bitcoin.prototype.generateBlock = function(num, callback) {
  throw new Error('generateBlock not implemented');
};

/**
 * Called by Node to stop the service.
 * @param {Function} callback
 */
Bitcoin.prototype.stop = function(callback) {
  this.stopping = true;
  log.info('[bitcoin] stopping in 1 sec');
  setTimeout(callback, 1e3);
};

module.exports = Bitcoin;
