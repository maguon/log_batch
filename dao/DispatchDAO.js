'use strict'
const db = require('../db/connection/MysqlDb.js');

const serverLogger = require('../util/ServerLogger.js');
const logger = serverLogger.createLogger('DispatchDAO.js');

const queryTaskStat = (params,callback) => {

    const query = "select dts.id ,count(if(ddi.demand_status=1,true,null )) demand_count from dp_task_stat dts left join dp_demand_info ddi on " +
        " dts.route_start_id = ddi.route_start_id and dts.base_addr_id= ddi.base_addr_id " +
        " and dts.route_end_id= ddi.route_end_id and dts.receive_id = ddi.receive_id and dts.date_id = ddi.date_id " +
        " where task_stat_status <>2  group by dts.id  having demand_count=0 order by dts.id "
    let paramArray=[],i=0;
    db.dbQuery(query,paramArray,(error,result)=>{
        logger.debug(' queryTaskStat ')
        callback(error,result)
    });
}

const updateTaskStat = (params,callback) => {
    const query = "update dp_task_stat set task_stat_status = ? where id = ? " ;
    let paramArray=[],i=0;
    paramArray[i++]=params.dpTaskStatStatus;
    paramArray[i]=params.dpTaskStatId;
    db.dbQuery(query,paramArray,(error,result)=>{
        logger.debug(' updateTaskStat ')
        callback(error,result)
    });
}
const updateTaskStatByDate = (params,callback) =>{
    const query = "update dp_task_stat set task_stat_status = ? where task_stat_status = ? and date_id <= ? " ;
    let paramArray=[],i=0;
    paramArray[i++]=params.dpTaskStatStatus;
    paramArray[i++]=params.originDpTaskStatStatus;
    paramArray[i]=params.dateId;
    db.dbQuery(query,paramArray,(error,result)=>{
        logger.debug(' updateTaskStatByDate ')
        callback(error,result)
    });
}

const updateDemandByDate = (params,callback) => {
    const query = "update dp_demand_info set demand_status=2 where pre_count = plan_count and demand_status =1  and date_id=?" ;
    let paramArray=[],i=0;
    paramArray[i]=params.dateId;
    db.dbQuery(query,paramArray,(error,result)=>{
        logger.debug(' updateDemandByDate ')
        callback(error,result)
    });
}
module.exports = {
    queryTaskStat , updateTaskStat ,updateTaskStatByDate ,updateDemandByDate
}