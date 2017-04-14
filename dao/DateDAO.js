'use strict'
const db = require('../db/connection/MysqlDb.js');

var serverLogger = require('../util/ServerLogger.js');
var logger = serverLogger.createLogger('DateDAO.js');

var queryDate = function(params,callback){
    const querySelect = "select * from date_base  where " +
        "id=? and  day=? and week=? and month=? and year=? and y_month=? and y_week=? ";
    let paramArray=[],i=0;
    paramArray[i++]=params.id;
    paramArray[i++]=params.day;
    paramArray[i++]=params.week;
    paramArray[i++]=params.month;
    paramArray[i++]=params.year;
    paramArray[i++]=params.yearMonth;
    paramArray[i]=params.yearWeek;
    db.dbQuery(querySelect,paramArray,function(error,rows){
        logger.debug(' queryDate ');
        callback(error,rows);
    });
}

var createDate = function(params,callback){
    const query='insert into date_base (`id`,`day`,`week`,`month`,`year`,`y_month`,`y_week`) values (?,?,?,?,?,?,?);'
    let paramArray=[],i=0;
    paramArray[i++]=params.id;
    paramArray[i++]=params.day;
    paramArray[i++]=params.week;
    paramArray[i++]=params.month;
    paramArray[i++]=params.year;
    paramArray[i++]=params.yearMonth;
    paramArray[i]=params.yearWeek;

    db.dbQuery(query,paramArray,function(error,result){
        logger.debug(' createDate ')
        callback(error,result)
    });
}

const initStorageStat = (callback)=>{
    const query="insert into storage_stat_date (date_id,storage_id,balance) " +
        "  (select DATE_FORMAT(CURRENT_DATE(),'%Y%m%d') as date_id ,ssd.storage_id ,ssd.balance " +
        " from storage_stat_date ssd left join storage_info si on ssd.storage_id=si.id " +
        " where ssd.date_id = DATE_FORMAT(CURRENT_DATE(),'%Y%m%d')-1 and si.storage_status=1) ";
    db.dbQuery(query,[],function(error,result){
        logger.debug(' initStorageStat ')
        callback(error,result)
    });
}

module.exports = {queryDate , createDate,initStorageStat}