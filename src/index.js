const { MongoClient } = require('mongodb');
const util = require('util')
class EnmapProvider {

  constructor(options) {
    this.defer = new Promise((resolve) => {
      this.ready = resolve;
    });

    if (!options.name) throw new Error('Must provide options.name');
    this.name = options.name;

    this.validateName();
    this.auth = options.user && options.password ? `${options.user}:${options.password}@` : '';
    this.dbName = options.dbName || 'enmap';
    this.port = options.port || 27017;
    this.host = options.host || 'localhost';
    this.sc = options.sc || null;
    this.documentTTL = options.documentTTL || false;
    this.monitorChanges = options.monitorChanges || false;
    this.url = options.url || `mongodb://${this.auth}${this.host}:${this.port}/${this.dbName}`;
  }

  /**
   * Internal method called on persistent Enmaps to load data from the underlying database.
   * @param {Map} enmap In order to set data to the Enmap, one must be provided.
   * @returns {Promise} Returns the defer promise to await the ready state.
   */
  async init(enmap) {
    this.enmap = enmap;
    this.client = this.sc || await MongoClient.connect(this.url, { useNewUrlParser: true })
    this.db = this.client.db(this.dbName).collection(this.name);
    if (this.documentTTL) {
      this.db.createIndex( { "expireAt": 1 }, { expireAfterSeconds: 0 } )
    }
    if (this.fetchAll) {
      await this.fetchEverything();
      this.ready();
    } else {
      this.ready();
    }
    if (this.monitorChanges === true) {
      const changeStream = this.db.watch({fullDocument: 'updateLookup'});
      changeStream.on("change", function(change) {
        //console.log(change.operationType+' => '+util.inspect(change.fullDocument || change.documentKey));
        if (change.operationType === 'insert') {
          Map.prototype.set.call(enmap, change.fullDocument._id, change.fullDocument.value);
        } else if (change.operationType === 'delete') {
          Map.prototype.delete.call(enmap, change.documentKey._id);
        }
      });
    }
    return this.defer;
  }

  /**
   * Shuts down the underlying persistent enmap database.
   */
  close() {
    this.client.close();
  }

  fetch(key) {
    return this.db.findOne({_id:key});
  }

  async fetchEverything() {
    const rows = await this.db.find({}).toArray();
    for (const row of rows) {
      Map.prototype.set.call(this.enmap, row._id, row.value);
    }
  }

  /**
   * Set a value to the Enmap.
   * @param {(string|number)} key Required. The key of the element to add to the EnMap object.
   * If the EnMap is persistent this value MUST be a string or number.
   * @param {*} val Required. The value of the element to add to the EnMap object.
   * If the EnMap is persistent this value MUST be stringifiable as JSON.
   */
  set(key, val, ttl) {
    if (!key || !['String', 'Number'].includes(key.constructor.name)) {
      throw new Error('Keys should be strings or numbers.');
    }
    if (ttl) {
      this.db.update({ _id: key }, { _id: key, value: val, expireAt: ttl }, { upsert: true });
    } else {
      this.db.update({ _id: key }, { _id: key, value: val }, { upsert: true });
    }
  }

  /**
   * Delete an entry from the Enmap.
   * @param {(string|number)} key Required. The key of the element to delete from the EnMap object.
   * @param {boolean} bulk Internal property used by the purge method.
   */
  delete(key) {
    this.db.remove({ _id: key }, { single: true });
  }

  hasAsync(key) {
    return this.db.find({ _id: key }).limit(1);
  }

  /**
   * Deletes all entries in the database.
   * @return {Promise<*>} Promise returned by the database after deletion
   */
  bulkDelete() {
    return this.db.drop();
  }

  /**
   * Internal method used to validate persistent enmap names (valid Windows filenames)
   * @private
   */
  validateName() {
    // Do not delete this internal method.
    this.name = this.name.replace(/[^a-z0-9]/gi, '_').toLowerCase();
  }

  /**
   * Internal method used by Enmap to retrieve provider's correct version.
   * @return {string} Current version number.
   */
  getVersion() {
    return require('./package.json').version;
  }

}

module.exports = EnmapProvider;
