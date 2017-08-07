'use strict'
const Promise = require('promise');
const gpsDao = require('../dao/GpsDAO.js');
const serverLogger = require('../util/ServerLogger.js');
const logger = serverLogger.createLogger('Gps.js');


const saveBizTruck = (callback) =>{
    const queryBizTruck = ()=>{
        return new Promise((resolve,reject)=>{
            gpsDao.getBizTruck((error,result)=>{
                if(error){
                    reject(error)
                }else{
                    logger.info('saveBizTruck '+ "getBizTruck success")
                    let simArray = result.simArray;
                    let truckArray = result.truckArray;
                    let truckSimArray =[]
                    for(let i=0;i<simArray.length;i++){
                        let truckSim ={
                            truck:truckArray[i].replace('.','').replace('。','').replace("'",''),
                            sim:simArray[i].replace("'",'')
                        }
                        truckSimArray.push(truckSim);
                    }
                    resolve(truckSimArray);
                }
            })
        })
    };

    const getTruckBase = (simObj) =>{
        return new Promise((resolve,reject)=>{
            gpsDao.getTruckBase(simObj,(error,result)=>{
                if(error){
                    reject(error);
                }else{
                    resolve(result);
                }
            })
        })
    }

    const saveTruckGps = (truckObj)=>{
        return new Promise((resolve,reject)=>{
            gpsDao.saveTruck(truckObj,(error,result)=>{
                if(error){
                    reject(error);
                }else{
                    resolve(result);
                }
            })
        })
    }
    queryBizTruck().then((truckArray)=>{
        /*Promise.map(truckArray,(truck,i)=>{
            console.log(truck,i);
        })*/
        Promise.all(truckArray.map((item)=>{

            getTruckBase({sim:item.sim}).then((result)=>{
                logger.info('saveBizTruck' + ' getTruck success' +item.sim);

                var truckParams = {
                    vheNo: result.PosInfo[0].VehNoF.replace('.','').replace('。','').replace("'",''),
                    sim : result.PosInfo[0].SimID ,
                    lat: result.PosInfo[0].Latitude,
                    lon : result.PosInfo[0].Longitude,
                    vel : result.PosInfo[0].Velocity,
                    oil : result.PosInfo[0].Oil,
                    angle : result.PosInfo[0].Angle,
                    mileage : result.PosInfo[0].Miles,
                    updateDate : new Date(result.PosInfo[0].DateTime)
                }
                return saveTruckGps(truckParams)
            }).then((result)=>{
                logger.info("saveBizTruck" + " saveTruck success "+result);
                return result;
            })
        })).then((data)=>{
            console.log('data:'+data);
        })
    })



}



module.exports = {
    saveBizTruck
}