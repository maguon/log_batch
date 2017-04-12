const MongoClient = require('mongodb').MongoClient;
const Server = require('mongodb').Server;
const serverLogger = require('../../util/ServerLogger');
const sysConfig = require('../../config/SystemConfig');
const logger = serverLogger.createLogger('MongoCon');



let db= null;
MongoClient.connect(sysConfig.mongoConfig.connect,(err, dbInstance) => {
    if(err){
        logger.error(' connect Mongodb failed ' + err.message);

    }else{
        db= dbInstance;
    }

});

const getDB= (callback) =>{
    // Open the connection to the server
    if (db==null){
        logger.info(' getDb ' + 'attempt to create mongodb connection ')

        MongoClient.connect(sysConfig.mongoConfig.connect,(err, dbInstance) => {
            if(err){
                logger.error(' connect Mongodb failed ' + err.message);
                return callback(err,null);
            }else{
                return  callback(null,dbInstance);
            }

        });
    }else {
        callback(null, db);
    }

};



module.exports = {
    getDb: getDB
};

