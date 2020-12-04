const Seq = require('seq');
const fs = require('fs');
const sysMsg = require('./util/SystemMsg.js');
const sysError = require('./util/SystemError.js');
const serverLogger = require('./util/ServerLogger.js');
const logger = serverLogger.createLogger('GetUpDistanceFile.js');
//const dispatchDAO = require('./dao/DispatchDAO');
const moment = require('moment');
const db = require('./db/connection/MysqlDb.js');
const updateTwoOutputByMonth = (params,callback) => {
    const yMonth =process.argv[3];
    logger.info('update '+yMonth);
    var query = " update drive_truck_month_value dtmv inner join( " +
        " select dprt.drive_id,dprt.truck_id, " +
        " sum(ecrr.two_fee*ecrr.two_distance*drlt.output_ratio) two_output " +
        " from dp_route_task dprt " +
        " left join dp_route_load_task_detail drltd on drltd.dp_route_task_id = dprt.id " +
        " left join dp_route_load_task drlt on drlt.id = drltd.dp_route_load_task_id " +
        " left join car_info ci on drltd.car_id = ci.id " +
        " left join entrust_city_route_rel ecrr on ci.entrust_id = ecrr.entrust_id " +
        " where dprt.task_plan_date>= "+yMonth+"01 and dprt.task_plan_date<="+yMonth+"31 " +
        " and ci.make_id = ecrr.make_id and ci.route_start_id = ecrr.route_start_id and ci.route_end_id = ecrr.route_end_id and ci.size_type =ecrr.size_type  " +
        " and dprt.task_status >=9 " +
        " group by dprt.drive_id,dprt.truck_id) dprm " +
        " on dtmv.drive_id = dprm.drive_id and dtmv.truck_id = dprm.truck_id " +
        " and dtmv.y_month = "+yMonth+" set  dtmv.two_output = dprm.two_output ";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateTwoOutputByMonth ');
       logger.info(error||rows);
    });
}

/*
let csvString = ''
Seq().seq(function () {
    var that =this;
    dispatchDAO.getUpDistanceTask({},function(error,rows){
        if (error) {
            logger.error(' getUpDistanceTask ' + error.message);
            throw sysError.InternalError(error.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
        } else {
            Seq(rows).seqEach(function(rowObj,i){
                var that = this;
                csvString += rowObj.id+',';
                console.log(rowObj.id);
                dispatchDAO.getRouteRecord({routeId:rowObj.id},function(error,rows){
                    if (error) {
                        logger.error(' getUpRecord ' + error.message);
                        throw sysError.InternalError(error.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
                    } else {
                        const strArray = rows[0].comment;
                        let strTmp = '';
                        strArray.forEach((obj,i)=>{
                            strTmp += moment(obj.timez).format('YYYY-MM-DD HH:mm:ss') + ' ' +obj.name +' '+obj.content +' |';
                            console.log(obj);
                        })

                        csvString += strTmp +'\r\n';
                        logger.info(strTmp);
                        that(null,i);
                    }
                })
            }).seq(function(){
                that();
            });

        }
    })
}).seq(function(){
    fs.writeFile('/root/UpDistanceCsv',csvString,function(error){
        console.log(error);
    })
})*/
updateTwoOutputByMonth();