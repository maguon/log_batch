

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

const gpsHost = {
    host : '139.224.65.40',
    port : 80,
    cookie :'ASP.NET_SessionId=klba3s45ovlmtqjakimq4f45',
    bizTruckUrl : '/webgps/ajax/VehicleMonitor,App_Web_vehiclemonitor.aspx.cdcab7d2.qk3nmbls.ashx?_method=GetVehJson&_session=r',
    truckBaseUrl:'/webgps/ajax/NetUI_BaiduMap,App_Web_xjpbtqbl.ashx?_method=GetVehicleRef&_session=r'
}

module.exports = { mysqlConnectOptions ,loggerConfig, logLevel , mongoConfig  ,gpsHost }
