'use strict'
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

module.exports = {
    completeTaskStat
}