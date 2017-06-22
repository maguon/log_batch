'use strict'
const http = require('http');
const mongoose = require('../db/connection/MongoCon.js').getMongo();
const sysConfig = require('../config/SystemConfig');
const truckEntity = require('./schema/TruckSchema.js').truckEntity ;
const truckModel = mongoose.model('truck_gps', truckEntity);
const serverLogger = require('../util/ServerLogger.js');
const logger = serverLogger.createLogger('GpsDAO.js');

const getBizTruck = (callback)=>{
    let paramStr = JSON.stringify({});
    let options = {
        host: sysConfig.gpsHost.host,
        port: sysConfig.gpsHost.port,
        path: sysConfig.gpsHost.bizTruckUrl,
        method: 'post',
        headers: {
            'Content-Type': 'application/json',
            'Cookie' : sysConfig.gpsHost.cookie,
            'Content-Length' : Buffer.byteLength(paramStr, 'utf8')
        }
    }
    try{
        let req = http.request(options, (res) => {
            let data = "";
            res.on('data', (d)=>{
                data += d;
            });
            res.on('end', () =>{
                let originArray = data.split('|');
                let simArray = originArray[0].split(',');
                let truckArray = originArray[1].split(',');
                let result = {
                    simArray : simArray,
                    truckArray : truckArray
                }
                callback(null,result);


                //let resObj = eval("(" + data + ")");
                //console.log(resObj)
            });
        });
        req.write(paramStr);
        req.end();
        req.on('error', (e) =>{
            callback(e,null);
        });
    }catch(e){
        callback(e,null)
    }
}
const getTruckBase = (params,callback)=>{
    let paramStr = 'SystemNos='+params.sim;
    let options = {
        host: sysConfig.gpsHost.host,
        port: sysConfig.gpsHost.port,
        path: sysConfig.gpsHost.truckBaseUrl,
        method: 'post',
        headers: {
            'Content-Type': 'text/plain',
            'Cookie' : sysConfig.gpsHost.cookie,
            'Content-Length' : Buffer.byteLength(paramStr, 'utf8')
        }
    }
    try{
        let req = http.request(options, (res) => {
            let data = "";
            res.on('data', (d)=>{
                data += d;
            });
            res.on('end', () =>{
                /*let originArray = data.split('|');
                let simArray = originArray[0].split(',');
                let truckArray = originArray[1].split(',');
                let result = {
                    simArray : simArray,
                    truckArray : truckArray
                }*/
                console.log(data)
                let dataParase = eval("(" + data + ")");
                let resObj = eval("(" + dataParase + ")");

                callback(null,resObj);


                //let resObj = eval("(" + data + ")");
                //console.log(resObj)
            });
        });
        req.write(paramStr);
        req.end();
        req.on('error', (e) =>{
            callback(e,null);
        });
    }catch(e){
        callback(e,null)
    }
}

const saveTruck = (params,callback)=>{
    /*var truckObj = new truckModel({
        vhe_no : params.vheNo,
        sim : params.sim ,
        lat: params.lat,
        lon : params.lon,
        vel : params.vel,
        oil : params.oil,
        angle : params.angle,
        mileage : params.mileage,
        updated_on : params.updateDate
    })
    truckObj.findOneAndUpdate(function(error,result){
        logger.debug('saveTruck') ;
        callback(error,result);
    })*/
    const query = {vhe_no:params.vheNo,sim:params.sim};
    const update = {
        lat: params.lat,
        lon : params.lon,
        vel : params.vel,
        oil : params.oil,
        angle : params.angle,
        mileage : params.mileage,
        updated_on : params.updateDate
    }
    truckModel.findOneAndUpdate(query,update,{upsert:true},function(error,result){
        callback(error,result);
    })

}


module.exports = {
    getBizTruck ,getTruckBase ,saveTruck
}