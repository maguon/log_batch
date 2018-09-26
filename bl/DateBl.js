'use strict'
const Promise = require('promise');
const dateDao = require('../dao/DateDAO.js');
const dateUtil = require('../util/DateUtil.js');
const serverLogger = require('../util/ServerLogger.js');
const logger = serverLogger.createLogger('StatDate.js');

const saveStatDate = (callback) =>{
    let dateObj = {};
    const today = new Date();
    dateObj.day = today.getDate();
    dateObj.month = today.getMonth()+1;
    dateObj.year = today.getFullYear();
    dateObj.week = dateUtil.getWeekByDate();
    dateObj.yearMonth = Number(dateObj.year+dateUtil.padLeft(""+dateObj.month,2));
    dateObj.yearWeek = Number(dateObj.year+dateUtil.padLeft(""+dateObj.week,2));
    dateObj.id = Number(dateObj.yearMonth+dateUtil.padLeft(""+dateObj.day,2))
    const p = new Promise((resolve,reject)=>{
        dateDao.queryDate(dateObj,(err,rows)=>{
            resolve({err,rows})
        })
    }).then((result)=>{
        if(result.err){
            logger.error(' saveStatDate '+ result.err.message);
            callback(err,null);
        }else{
            if(result.rows && result.rows.length){
                logger.warn(' saveStatDate '+ ' new date has been exist');
                callback(null,{success:false});
            }else{
                dateDao.createDate(dateObj,(err,result) => {
                    if(err){
                        callback(err,result);
                    }else{
                        if(result && result.affectedRows){
                            callback(null,{success:true,id:dateObj.id})
                        }else{
                            callback(null,{success:false})
                        }
                    }
                })
            }
        }
    })
}

const initStorageStat =(callback)=>{
    dateDao.initStorageStat((err,result)=>{
        callback(err,result)
    })
}

module.exports = {
    saveStatDate : saveStatDate ,
    initStorageStat : initStorageStat
}