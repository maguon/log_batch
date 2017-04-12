

const mysqlConnectOptions ={
    user: 'log',
    password: 'log_base',
    database:'log_base',
    host: '127.0.0.1' ,
    charset : 'utf8mb4',
    //,dateStrings : 'DATETIME'
};


const logLevel = 'DEBUG';
const loggerConfig = {
    appenders: [
        { type: 'console' },
        {
            "type": "file",
            "filename": "../stage/log_batch.html",
            "maxLogSize": 2048000,
            "backups": 10
        }
    ]
}


const mongoConfig = {
    connect : 'mongodb://127.0.0.1:27017/log_records'
}


module.exports = { mysqlConnectOptions ,loggerConfig, logLevel , mongoConfig  }
