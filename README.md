# MongoDB Context for Node-RED

## Install

1. Run the following command in your Node-RED user directory - typically `~/.node-red`

    ```bash
    npm install git+https://github.com/node-red/node-red-context-redis
    ```

1. Add a configuration in `settings.js` under `contextStorage`:

    ```javascript
    contextStorage: {
        // ...
        mongodb: {
            module: require('node-red-context-redis'),
            config: {
                // see below options
            }
        }
    }
    ```

1. Restart Node-RED

## Options

These are the available options under `config` and their default values:

```javascript
config: {
    host:     '127.0.0.1' // hostname or IP address of the MongoDB server
    port:     '27017'     // port of the MongoDB server
    database: 'context'   // name of the database
    username: null        // username for authentication
    password: null        // password for authentication
    options:  null        // additional options for the driver as URL string
}
```