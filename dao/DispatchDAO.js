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

module.exports = {
    queryTaskStat , updateTaskStat
}