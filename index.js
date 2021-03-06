/**
 * Configuration:
 * {
 *    host:     127.0.0.1
 *    port:     27017
 *    database: context
 *    username: null
 *    password: null
 *    options:  null
 *  }
 */

const mongoose = require('mongoose')

const util = process.env.NODE_RED_HOME ?
    require(require.resolve('@node-red/util', {paths: [process.env.NODE_RED_HOME]})).util :
    require('@node-red/util').util;

const log = process.env.NODE_RED_HOME ?
    require(require.resolve('@node-red/util', {paths: [process.env.NODE_RED_HOME]})).log :
    require('@node-red/util').log;



function MongoContext(config) {
    this['host']     = config['host']     || '127.0.0.1'
    this['port']     = config['port']     || '27017',
    this['database'] = config['database'] || 'context',
    this['username'] = config['username'] || '',
    this['password'] = config['password'] || '',

    this['client']   = null // mongoose.connection
    this['models']   = {
        '_collections': mongoose.model('_collections', {
            _id  : String,
            date : mongoose.Schema.Types.Date
        })
    }   // mongoose.model
}


/**
 * Turns functions into strings in objects.
 * 
 * @param {any}   obj - The object with functions to stringify. 
 * @returns {any} The object with stringified functions.
 */
function stringifyFunctions(obj) {
    if (!!obj) {
        try {
            if (typeof obj === 'function') {
                obj = obj.toString()
            } else if (Array.isArray(obj)) {
                obj = obj.map(x => stringifyFunctions(x))
            } else if (typeof obj === 'object') {
                for (let key in obj) {
                    obj[key] = stringifyFunctions(obj[key])
                }
            }
        } catch (err) {
            console.error(err)
        }
    }

    return obj
}


/**
 * Evaluates a string into a function in objects.
 * 
 * @param {any}   obj - The object with functions to evaluate. 
 * @returns {any} The object with evaluated functions.
 */
function evaluateFunctions(obj) {
    if (!!obj) {
        try {
            if (typeof obj === 'string') {
                old_obj = obj
                obj = eval(obj)

                if (typeof obj !== 'function') {
                    obj = old_obj
                }
            } else if (Array.isArray(obj)) {
                obj = obj.map(x => evaluateFunctions(x))
            } else if (typeof obj === 'object') {
                for (let key in obj) {
                    obj[key] = evaluateFunctions(obj[key])
                }
            }
        } catch (err) {}
    }
    
    return obj
}


/**
 * Retrieves the mongoose model for the given collection.
 * If the model does not exist, it is created.
 * 
 * @param {string}           collection - The collection name.
 * @returns {mongoose.Model} The mongoose model.
 */
MongoContext.prototype.getModel = function(collection) {
    if (!this['models'].hasOwnProperty(collection)) {
        this['models'][collection] = mongoose.model(collection, {
            _id   : String,
            value : mongoose.Schema.Types.Mixed
        })

        this.set('_collections', collection, Date.now(), null)
    }

    return this['models'][collection]
}



/**
 * Open the storage ready for use. This is called before any store values are accessed.
 * 
 * @returns {Promise} A Promise that resolves when the store is ready for access.
 */
MongoContext.prototype.open = function() {
    console.log('\n[MONGODB CONTEXT] Opening MongoDB Context')

    let uri = 'mongodb://'

    if (this['username'] && this['password']) {
        uri += `${this['username']}:${this['password']}@`
    } else if (this['username']) {
        uri += `${this['username']}@`
    }

    if (typeof this['host'] === 'string') {
        uri += `${this['host']}:${this['port']}`
    } else if (Array.isArray(this['host'])) {
        for (let i = 0; i < this['host'].length; i++) {
            uri += `${this['host'][i]}:${this['port'][i]}`
            if (i < this['host'].length - 1) uri += ','
        }
    }

    uri += `/${this['database']}`

    if (this['options']) {
        uri += `?${this['options']}`
    }

    return new Promise((resolve, reject) => {
        this['client'] = mongoose.connect(uri, {
            socketTimeoutMS: 60000,
            keepAlive: true,
            keepAliveInitialDelay: 300000,
        })

        this['client'] = mongoose.connection

        this['client'].on('error', (err) => {
            console.error('\n[MONGODB CONTEXT] Failed to connect to MongoDB Context at ' + uri)
            console.error(err)
            reject(err)
        })

        this['client'].once('open', () => {
            console.log('\n[MONGODB CONTEXT] Connected to MongoDB Context at ' + uri)
            resolve()
            console.log('\n[MONGODB CONTEXT] MongoDB Context resolved')
        })
    })
}


/**
 * Called when the runtime is stopped so no further key values will be accessed.
 * 
 * @returns {Promise} A Promise that resolves when the store is closed.
 */
MongoContext.prototype.close = function() {
    console.log('\n[MONGODB CONTEXT] Closing MongoDB Context')
    
    return new Promise((resolve, reject) => {
        this['client'].close(true, err => {
            if (err) {
                console.error('\n[MONGODB CONTEXT] Failed to close MongoDB Context')
                console.error(err)
                reject(err)
            } else {
                console.log('\n[MONGODB CONTEXT] Closed MongoDB Context')
                resolve()
            }
        })
    })
}


/**
 * Get the value of a key in the scope (e.g. global or flow).
 * The key argument can be either a String identifying a single key, or an Array of Strings identifying multiple keys to return the values for.
 * If the optional callback argument is provided, it must be a function that takes two or more arguments.
 *   If no callback is provided, and the store supports synchronous access, the get function should return the individual value, or array of values for the keys.
 *   If the store does not support synchronous access it should throw an error.
 * 
 * @param {string}         scope    - The scope of the key.
 * @param {string | any[]} key      - The key, or array of keys, to return the value(s) for.
 * @param {function?}      callback - A callback function to invoke with the key value.
 */
MongoContext.prototype.get = function(scope, key, callback) {
    console.log(`[MONGODB CONTEXT] Getting value for key ${key} in scope ${scope}`)

    if (callback && typeof callback !== 'function') {
        throw new Error('Callback must be a function, got ' + typeof callback);
    }

    try {
        if (!Array.isArray(key)) {
            key = [key]
        }

        const keys       = key.filter((v, i, a) => a.indexOf(v) === i)
        const query      = {_id: {$in: keys}}
        const projection = {_id: false, value: true}
        const options    = {limit: keys.length}

        this.getModel(scope).find(query, projection, options, (err, docs) => {
            if (err) {
                console.error('\n[MONGODB CONTEXT] Failed to get key values from MongoDB Context')

                try {
                    callback(err)
                } catch (err2) {
                    console.error(err)
                }
            } else {
                const values = evaluateFunctions(docs.map(doc => doc['value']))

                console.log(`[MONGODB CONTEXT] Found values for keys ${keys}`)

                try {
                    callback(null, ...values)
                } catch (err2) {
                    console.error(`[MONGODB CONTEXT] Error after get success, callback ${callback}`)
                    console.error(err2)
                }
            }
        })
    } catch (err) {
        try {
            callback(err)
        } catch (err2) {
            console.error(err)
        }
    }
}


/**
 * Set the value of a key in the scope (e.g. global or flow).
 * If the optional callback argument is provided, it will be called when the value has been stored.
 *   It takes a single argument, error, to indicate any errors hit whilst storing the values.
 *   If no callback is provided, and the store supports synchronous access, the set function should return once the value is stored.
 *   If the store does not support synchronous access it should throw an error.
 * 
 * @param {string}         scope    - The scope of the key.
 * @param {string | any[]} key      - The key, or array of keys, to return the value(s) for.
 * @param {any}            value    - The value, or array of values.
 * @param {function?}      callback - A callback function to invoke with the key value.
 */
MongoContext.prototype.set = function(scope, key, value, callback) {
    console.log(`[MONGODB CONTEXT] Setting value for key ${key} in scope ${scope}`)

    if (callback && typeof callback !== 'function') {
        throw new Error('Callback must be a function, got ' + typeof callback)
    }

    try {
        if (!Array.isArray(key)) {
            key   = [key]
            value = [value]
        } if (!Array.isArray(value)) {
            value = [value]
        }

        if (key.length > value.length) {
            for (let i = value.length; i < key.length; i++) value.push(null)
        } else if (key.length < value.length) {
            for (let i = key.length; i < value.length; i++) key.push(null)
        }

        const pairs = stringifyFunctions(key.map(function(k, i) {
            return {'_id': k.toString(), 'value': value[i]}
        })).map(function(doc) {
            return {
                updateOne: {
                    filter: {_id: doc['_id']},
                    update: doc,
                    upsert: true,
                }
            }
        })

        const options = {
            ordered: false,
            skipValidation: true,
        }
        this.getModel(scope).bulkWrite(pairs, options, (err, result) => {
            if (err) {
                console.error('\n[MONGODB CONTEXT] Failed to set key/value pair in MongoDB Context')

                try {
                    callback(err)
                } catch (err2) {
                    console.error(err)
                }
            } else {
                console.log('\n[MONGODB CONTEXT] Set key/value pair in MongoDB Context')
            }
        })
    } catch (err) {
        try {
            callback(err)
        } catch (err2) {
            console.error(err)
        }
    }
}


/**
 * Gets a list of all keys under the given scope.
 * If the optional callback argument is provided, it must be a function that takes two or more arguments.
 *   If no callback is provided, and the store supports synchronous access, the keys function should return the array of keys.
 *   If the store does not support synchronous access it should throw an error.
 * 
 * @param {string}    scope    - The scope of the key.
 * @param {function?} callback - A callback function to invoke with the key value.
 */
MongoContext.prototype.keys = function(scope, callback) {
    console.log(`[MONGODB CONTEXT] Getting keys for scope ${scope}`)

    if (callback && typeof callback !== 'function') {
        throw new Error('Callback must be a function, got ' + typeof callback);
    }

    try {
        this.getModel(scope).find({}, {_id: true, value: false}, {}, (err, docs) => {
            if (err) {
                console.error('\n[MONGODB CONTEXT] Failed to find keys from MongoDB Context')
                console.error(err)
                try {
                    callback(err)
                } catch (err2) {
                    console.error(err)
                }
            } else {
                const values = docs.map(doc => doc['_id'])
                callback(null, values)
            }
        })
    } catch (err) {
        console.error('\n[MONGODB CONTEXT] Failed to get keys from MongoDB Context')
        console.error(err)
        try {
            callback(err)
        } catch (err2) {
            console.error(err)
        }
    }
}


/**
 * Deletes a whole scope.
 * 
 * @param   {string}  scope - The scope to delete.
 * @returns {Promise} A Promise that resolves when the store is closed.
 */
MongoContext.prototype.delete = function(scope) {
    console.log(`[MONGODB CONTEXT] Deleting scope ${scope}`)

    const collection = scope

    return new Promise((resolve, reject) => {
        this.getModel(scope).deleteMany({}, (err, result) => {
            if (err) {
                console.error('\n[MONGODB CONTEXT] Failed to delete scope from MongoDB Context')
                console.error(err)
                reject(err)
            } else {
                resolve()
            }
        })
    })
}


/**
 * Deletes the scopes that are not in the given list.
 * Returns a promise that resolves when store has removed any context scopes that are no longer required.
 * The activeNodes list can be used to identify what nodes and flows are still considered active.
 * 
 * @param   {any[]}   activeNodes - A list of all node/flow ids that are still active
 * @returns {Promise} A Promise that resolves when the store is closed.
 */
MongoContext.prototype.clean = function(activeNodes) {
    console.log('\n[MONGODB CONTEXT] Cleaning MongoDB Context')

    return new Promise((resolve, reject) => {
        var collections = []
        
        this['client'].db.listCollections().toArray((err, result) => {
            if (err) {
                console.error('\n[MONGODB CONTEXT] Failed to list MongoDB Context')
                console.error(err)
                reject(err)
            }

            collections = result
        })

        if (collections.length > 0) {
            const inactiveNodes = collections.filter(collection => !activeNodes.includes(collection))
    
            if (inactiveNodes.length > 0) {
                const promises = inactiveNodes.map(collection => this.delete(collection))
    
                Promise.all(promises).then(function() {
                    console.log(`[MONGODB CONTEXT] Cleaning collections ${inactiveNodes} from ${collections}`)
                    resolve()
                }).catch(function(err) {
                    console.error('\n[MONGODB CONTEXT] Failed to clean MongoDB Context')
                    console.error(err)
                    reject(err)
                })
            } else {
                resolve()
            }
        } else {
            resolve()
        }
    })
}



module.exports = function (config) {
    return new MongoContext(config)
}