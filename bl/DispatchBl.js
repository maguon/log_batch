'use strict'
const moment = require('moment/moment.js');
const Promise = require('promise');
const dispatchDao = require('../dao/DispatchDAO.js');
const serverLogger = require('../util/ServerLogger.js');
const logger = serverLogger.createLogger('DispatchBl.js');


const completeTaskStat = () =>{
    const getCompleteTaskStat = () =>{
        return new Promise((resolve,reject)=>{
            dispatchDao.queryTaskStat({},(error,result)=>{
                if(error){
                    reject(error);
                }else{
                    resolve(result);
                }
            })
        })
    }

    const updateTaskStatStatus = (taskStatObj)=>{
        return new Promise((resolve,reject)=>{
            dispatchDao.updateTaskStat(taskStatObj,(error,result)=>{
                if(error){
                    reject(error);
                }else{
                    resolve(result);
                }
            })
        })
    }
    getCompleteTaskStat().then((taskStatArray)=>{
        Promise.all(taskStatArray.map((item)=>{
            updateTaskStatStatus({dpTaskStatId:item.id,dpTaskStatStatus:2}).then((result)=>{
                logger.info('update Task status' + '  success' +item.id);
                return result;
            }).then((result)=>{
                logger.info("saveBizTruck" + " saveTruck success "+result);
                return result;
            })
        })).then((data)=>{
            console.log('complete :'+data);
        })
    });

}

const completeTaskStatByDate  = ()=>{
    let dateLineTime = new Date().getTime() - 7*24*60*60*1000;
    let dateLine = new Date(dateLineTime);
    const dateId = moment(dateLine).format('YYYYMMDD');
    let paramsObj = {
        dpTaskStatStatus : 2 ,
        originDpTaskStatStatus : 1,
        dateId : dateId
    }
    dispatchDao.updateTaskStatByDate(paramsObj,(error,result)=>{
        if(error){
            reject(error);
        }else{
            resolve(result);
        }
    })
}

module.exports = {
    completeTaskStat,completeTaskStatByDate
}