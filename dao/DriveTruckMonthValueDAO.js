/**
 * Created by zwl on 2019/5/29.
 */

const moment = require('moment');
const db=require('../db/connection/MysqlDb.js');
const serverLogger = require('../util/ServerLogger.js');
const logger = serverLogger.createLogger('DriveTruckMonthValueDAO.js');


function addDistance(params,callback){
    var query = " insert into drive_truck_month_value" +
        " (y_month,drive_id,truck_id,reverse_count,load_distance,no_load_distance,distance) " +
        " select "+params.yMonth+",dpr.drive_id,dpr.truck_id, " +
        " count(case when dpr.reverse_flag = 1 then dpr.id end) as reverse_count, " +
        " sum(case when dpr.load_flag = 1 then dpr.distance end) as load_distance, " +
        " sum(case when dpr.load_flag = 0 then dpr.distance end) as no_load_distance, " +
        " sum(dpr.distance) as distance " +
        " from dp_route_task dpr " +
        " left join truck_info t on dpr.truck_id = t.id " +
        " left join truck_brand tb on t.brand_id = tb.id " +
        " where dpr.task_status >=9 " +
        " and dpr.task_plan_date>="+params.yMonth+"01 and dpr.task_plan_date<=" +params.yMonth+"31"+
        " group by dpr.drive_id,dpr.truck_id ";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' addDistance ');
        return callback(error,rows);
    });
}

function updateStorageCarCount(params,callback){
    var query = " update drive_truck_month_value dtmv inner join( " +
        " select dpr.drive_id,dpr.truck_id, " +
        " sum( case when dprl.receive_flag=0 and dprl.transfer_flag=0 then dprl.real_count end) not_storage_car_count, " +
        " sum( case when dprl.receive_flag=1 or dprl.transfer_flag=1 then dprl.real_count end) storage_car_count, " +
        " sum( case when dprl.receive_flag=0 and dprl.transfer_flag=0 then dprl.real_count end)*4 as enter_fee " +
        " from dp_route_task dpr " +
        " left join dp_route_load_task dprl on dpr.id = dprl.dp_route_task_id " +
        " where dpr.task_status >=9 and dpr.task_plan_date>="+params.yMonth+"01 and dpr.task_plan_date<="+params.yMonth+"31 " +
        " group by dpr.drive_id,dpr.truck_id) dprm " +
        " on dtmv.drive_id = dprm.drive_id and dtmv.truck_id = dprm.truck_id and dtmv.y_month = " +params.yMonth+
        " set dtmv.receive_car_count = dprm.not_storage_car_count, " +
        " dtmv.storage_car_count = dprm.storage_car_count , dtmv.enter_fee = dprm.enter_fee";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateStorageCarCount ');
        return callback(error,rows);
    });
}

function updateOutput(params,callback){
    var query = " update drive_truck_month_value dtmv inner join( " +
        " select dprt.drive_id,dprt.truck_id,sum(ecrr.fee*ecrr.distance*drlt.output_ratio) output " +
        " from dp_route_task dprt " +
        " left join dp_route_load_task_detail drltd on drltd.dp_route_task_id = dprt.id " +
        " left join dp_route_load_task drlt on drlt.id = drltd.dp_route_load_task_id " +
        " left join car_info ci on drltd.car_id = ci.id " +
        " left join entrust_city_route_rel ecrr on ci.entrust_id = ecrr.entrust_id " +
        " and ci.make_id = ecrr.make_id and ci.route_start_id = ecrr.route_start_id and ci.route_end_id = ecrr.route_end_id " +
        " and ci.size_type =ecrr.size_type  " +
        " where dprt.task_plan_date>= "+params.yMonth+"01 and dprt.task_plan_date<="+params.yMonth+"31 " +
        " and dprt.task_status >=9 " +
        " group by dprt.drive_id,dprt.truck_id) dprm " +
        " on dtmv.drive_id = dprm.drive_id and dtmv.truck_id = dprm.truck_id " +
        " and dtmv.y_month = "+params.yMonth+" set dtmv.output = dprm.output ";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateOutput ');
        return callback(error,rows);
    });
}

function updateInsureFee(params,callback){
    var lastDay = moment(params.yMonth+'01').endOf('month').format("YYYYMMDD");
    var query = " update drive_truck_month_value dtmv inner join( " +
        " select truck_id, sum(case when start_date<="+params.yMonth+"01 and end_date>="+lastDay+" then 30/DateDiff(end_date,start_date)*insure_money " +
        " when (start_date<="+params.yMonth+"01 and end_date<="+lastDay+" and end_date>="+params.yMonth+"01) then DateDiff(end_date,"+params.yMonth+"01)/DateDiff(end_date,start_date)*insure_money " +
        " when (start_date>="+params.yMonth+"01 and start_date<="+lastDay+" and end_date>="+lastDay+") then DateDiff("+lastDay+",start_date)/DateDiff(end_date,start_date)*insure_money end) month_insure" +
        " from truck_insure_rel " +
        " where (start_date<="+params.yMonth+"01 and end_date>="+lastDay+") or (start_date<="+params.yMonth+"01 and end_date<="+lastDay+" and end_date>="+params.yMonth+"01) " +
        " or (start_date>="+params.yMonth+"01 and start_date<="+lastDay+" and end_date>="+lastDay+") " +
        " group by truck_id) tir " +
        " on dtmv.truck_id = tir.truck_id " +
        " and dtmv.y_month = "+params.yMonth+" set dtmv.insure_fee = tir.month_insure ";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateInsureFee ');
        return callback(error,rows);
    });
}

function updateDistanceSalary(params,callback){
    var query = " update drive_truck_month_value dtmv inner join( " +
        " select dpr.drive_id,dpr.truck_id, " +
        " sum( case " +
        " when dpr.reverse_flag=0 and dpr.truck_number=6 and dpr.car_count<=3 then dpr.distance*0.6 " +
        " when dpr.reverse_flag=0 and dpr.truck_number=6 and dpr.car_count=4 then dpr.distance*0.7 " +
        " when dpr.reverse_flag=0 and dpr.truck_number=6 and dpr.car_count=5 then dpr.distance*0.8 " +
        " when dpr.reverse_flag=0 and dpr.truck_number=6 and dpr.car_count=6 then dpr.distance*0.9 " +
        " when dpr.reverse_flag=0 and dpr.truck_number=6 and dpr.car_count>=7 then dpr.distance " +
        " when dpr.reverse_flag=0 and dpr.truck_number=8 and dpr.car_count<5 then dpr.distance*0.6 " +
        " when dpr.reverse_flag=0 and dpr.truck_number=8 and dpr.car_count=5 then dpr.distance*0.7 " +
        " when dpr.reverse_flag=0 and dpr.truck_number=8 and dpr.car_count=6 then dpr.distance*0.8 " +
        " when dpr.reverse_flag=0 and dpr.truck_number=8 and dpr.car_count=7 then dpr.distance*0.9 " +
        " when dpr.reverse_flag=0 and dpr.truck_number=8 and dpr.car_count=8 then dpr.distance " +
        " when dpr.reverse_flag=0 and dpr.truck_number=8 and dpr.car_count=9 then dpr.distance*1.1 " +
        " when dpr.reverse_flag=0 and dpr.truck_number=8 and dpr.car_count>=10 then dpr.distance*1.2 " +
        " end) distance_salary, " +
        " sum(case when dpr.reverse_flag=1 then dpr.reverse_money end) reverse_salary " +
        " from dp_route_task dpr " +
        " where dpr.task_plan_date>="+params.yMonth+"01 and dpr.task_plan_date<="+params.yMonth+"31 " +
        " and dpr.task_status>=9 " +
        " group by dpr.drive_id,dpr.truck_id) dprm " +
        " on dtmv.drive_id = dprm.drive_id and dtmv.truck_id = dprm.truck_id " +
        " and dtmv.y_month = "+params.yMonth+" set dtmv.distance_salary = dprm.distance_salary , dtmv.reverse_salary = dprm.reverse_salary ";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        return callback(error,rows);
    });
}

function updateDamage(params,callback){
    var query = " update drive_truck_month_value dtmv inner join( " +
        " select di.drive_id,di.truck_id,sum(dc.under_cost) damage_under_fee,sum(dc.company_cost) damage_company_fee " +
        " from damage_check dc " +
        " left join damage_info di on dc.damage_id = di.id " +
        " where dc.date_id>="+params.yMonth+"01 and dc.date_id<="+params.yMonth+"31 and di.damage_status =3 " +
        " group by di.drive_id,di.truck_id)dim " +
        " on dtmv.drive_id = dim.drive_id and dtmv.truck_id = dim.truck_id and dtmv.y_month ="+params.yMonth+" " +
        " set dtmv.damage_under_fee = dim.damage_under_fee , dtmv.damage_company_fee = dim.damage_company_fee ";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateDamage ');
        return callback(error,rows);
    });
}

function updateCleanFee(params,callback){
    var query = " update drive_truck_month_value dtmv inner join( " +
        " select drcr.drive_id,drcr.truck_id," +
        " sum(drcr.total_price) total_clean_fee, " +
        " sum(drcr.total_trailer_fee) total_trailer_fee, " +
        " sum(drcr.car_parking_fee) car_parking_fee, " +
        " sum(drcr.total_run_fee) total_run_fee, " +
        " sum(drcr.lead_fee) lead_fee " +
        " from dp_route_load_task_clean_rel drcr " +
        " where drcr.date_id>="+params.yMonth+"01 and drcr.date_id<="+params.yMonth+"31 and drcr.status=2 " +
        " group by drcr.drive_id,drcr.truck_id) drcrm " +
        " on dtmv.drive_id = drcrm.drive_id and dtmv.truck_id = drcrm.truck_id and dtmv.y_month = " +params.yMonth+
        " set dtmv.clean_fee = drcrm.total_clean_fee , dtmv.trailer_fee = drcrm.total_trailer_fee , " +
        " dtmv.car_parking_fee = drcrm.car_parking_fee , dtmv.run_fee = drcrm.total_run_fee, " +
        " dtmv.lead_fee = drcrm.lead_fee ";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateCleanFee ');
        return callback(error,rows);
    });
}

function updateHotelFee(params,callback){
    var query = " update drive_truck_month_value dtmv inner join( " +
        " select drive_id,truck_id,sum(work_count) work_count,sum(hotel_fee) hotel_fee " +
        " from drive_work " +
        " where y_month = " +params.yMonth+
        " group by drive_id,truck_id) dw on dtmv.drive_id = dw.drive_id and dtmv.truck_id = dw.truck_id" +
        " and dtmv.y_month = "+params.yMonth+" " +
        " set dtmv.work_count = dw.work_count , dtmv.hotel_fee = dw.hotel_fee ";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateHotelFee ');
        return callback(error,rows);
    });
}

function updateEtcFee(params,callback){
    var query = " update drive_truck_month_value dtmv inner join( " +
        " select te.drive_id,te.truck_id,sum(te.etc_fee) etc_fee " +
        " from truck_etc te " +
        " where te.date_id>="+params.yMonth+"01 and te.date_id<= "+params.yMonth+"31 " +
        " group by te.drive_id,te.truck_id) tem " +
        " on dtmv.drive_id = tem.drive_id and dtmv.truck_id = tem.truck_id " +
        " and dtmv.y_month = "+params.yMonth+" set dtmv.etc_fee = tem.etc_fee ";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateEtcFee ');
        return callback(error,rows);
    });
}

function updateOilFee(params,callback){
    var query = " update drive_truck_month_value dtmv inner join( " +
        " select deor.drive_id,deor.truck_id,sum(deor.oil_money) oil_fee,sum(deor.urea_money) urea_fee " +
        " from drive_exceed_oil_rel deor " +
        " where deor.date_id>="+params.yMonth+"01 and deor.date_id<="+params.yMonth+"31 " +
        " group by deor.drive_id,deor.truck_id) deorm " +
        " on dtmv.drive_id = deorm.drive_id and dtmv.truck_id = deorm.truck_id and dtmv.y_month = " +params.yMonth+
        " set dtmv.oil_fee = deorm.oil_fee , dtmv.urea_fee = deorm.urea_fee ";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateOilFee ');
        return callback(error,rows);
    });
}

function updatePeccancy(params,callback){
    var query = " update drive_truck_month_value dtmv inner join( " +
        " select dp.drive_id,dp.truck_id,sum(dp.under_money) peccancy_under_fee,sum(dp.company_money) peccancy_company_fee " +
        " from drive_peccancy dp " +
        " where dp.date_id>="+params.yMonth+"01 and dp.date_id<= "+params.yMonth+"31 " +
        " group by dp.drive_id,dp.truck_id) dpm " +
        " on dtmv.drive_id = dpm.drive_id and dtmv.truck_id = dpm.truck_id and dtmv.y_month =   "+params.yMonth+
        " set dtmv.peccancy_under_fee = dpm.peccancy_under_fee , dtmv.peccancy_company_fee = dpm.peccancy_company_fee ";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updatePeccancy ');
        return callback(error,rows);
    });
}

function updateRepair(params,callback){
    var query = " update drive_truck_month_value dtmv inner join( " +
        " select trr.drive_id,trr.truck_id,sum(trr.repair_money) repair_fee, " +
        " sum(trr.parts_money) parts_fee,sum(trr.maintain_money) maintain_fee " +
        " from truck_repair_rel trr " +
        " where trr.date_id>="+params.yMonth+"01 and trr.date_id<="+params.yMonth+"31 and trr.repair_status =1 " +
        " group by trr.drive_id,trr.truck_id) trrm " +
        " on dtmv.drive_id = trrm.drive_id and dtmv.truck_id = trrm.truck_id and dtmv.y_month = " +params.yMonth+
        " set dtmv.repair_fee = trrm.repair_fee , dtmv.parts_fee = trrm.parts_fee , dtmv.maintain_fee = trrm.maintain_fee ";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateRepair ');
        return callback(error,rows);
    });
}

function updateCarOilFee(params,callback){
    var query = " update drive_truck_month_value dtmv inner join( " +
        " select dprtf.drive_id,dprtf.truck_id,sum(dprtf.car_oil_fee) car_oil_fee,sum(dprtf.car_total_price) car_parking_total_fee," +
        " sum(dprtf.total_price) truck_parking_fee " +
        " from dp_route_task_fee dprtf " +
        " where dprtf.date_id>="+params.yMonth+"01 and dprtf.date_id<="+params.yMonth+"31 and dprtf.status=2 " +
        " group by dprtf.drive_id,dprtf.truck_id) dprtfm " +
        " on dtmv.drive_id = dprtfm.drive_id and dtmv.truck_id = dprtfm.truck_id and dtmv.y_month = " +params.yMonth+
        " set dtmv.car_oil_fee = dprtfm.car_oil_fee , dtmv.car_parking_total_fee = dprtfm.car_parking_total_fee , " +
        " dtmv.truck_parking_fee = dprtfm.truck_parking_fee ";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateCarOilFee ');
        return callback(error,rows);
    });
}

function updateTruckNum(params,callback){
    var query = " update drive_truck_month_value dtmv inner join( " +
        " select t.id,t.truck_num,tr.number,t.brand_id,tb.brand_name," +
        " t.operate_type,t.company_id,c.company_name,t.output_company_id,t.output_company_name, " +
        " tb.load_distance_oil,tb.no_load_distance_oil " +
        " from truck_info t " +
        " left join truck_info tr on t.rel_id = tr.id " +
        " left join truck_brand tb on t.brand_id = tb.id " +
        " left join company_info c on t.company_id = c.id " +
        " where t.truck_type = 1)tm " +
        " on dtmv.truck_id = tm.id and dtmv.y_month = " +params.yMonth+
        " set dtmv.truck_num = tm.truck_num , dtmv.truck_number = tm.number, " +
        " dtmv.brand_id = tm.brand_id , dtmv.brand_name = tm.brand_name , dtmv.operate_type = tm.operate_type , " +
        " dtmv.company_id = tm.company_id , dtmv.company_name = tm.company_name , " +
        " dtmv.output_company_id = tm.output_company_id , dtmv.output_company_name = tm.output_company_name , " +
        " dtmv.load_oil_distance = tm.load_distance_oil , dtmv.no_oil_distance = tm.no_load_distance_oil ";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateTruckNum ');
        return callback(error,rows);
    });
}

function updateDrive(params,callback){
    var query = " update drive_truck_month_value dtmv inner join( " +
        " select id,drive_name from drive_info) d " +
        " on dtmv.drive_id = d.id and dtmv.y_month = " +params.yMonth+
        " set dtmv.drive_name = d.drive_name ";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateDrive ');
        return callback(error,rows);
    });
}

function getDriveTruckMonthValue(params,callback) {
    var query = " select dtmv.* from drive_truck_month_value dtmv " +
        " where dtmv.id is not null ";
    var paramsArray=[],i=0;
    if(params.driveTruckMonthValueId){
        paramsArray[i++] = params.driveTruckMonthValueId;
        query = query + " and dtmv.id = ? ";
    }
    if(params.driveId){
        paramsArray[i++] = params.driveId;
        query = query + " and dtmv.drive_id = ? ";
    }
    if(params.truckId){
        paramsArray[i++] = params.truckId;
        query = query + " and dtmv.truck_id = ? ";
    }
    if(params.operateType){
        paramsArray[i++] = params.operateType;
        query = query + " and dtmv.operate_type = ? ";
    }
    if(params.companyId){
        paramsArray[i++] = params.companyId;
        query = query + " and dtmv.company_id = ? ";
    }
    if(params.yMonth){
        paramsArray[i++] = params.yMonth;
        query = query + " and dtmv.y_month = ? ";
    }
    query = query + ' order by dtmv.drive_id ';
    if (params.start && params.size) {
        paramsArray[i++] = parseInt(params.start);
        paramsArray[i++] = parseInt(params.size);
        query += " limit ? , ? "
    }
    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' getDriveTruckMonthValue ');
        return callback(error,rows);
    });
}

function updateTruckDepreciationFee(params,callback){
    var query = " update drive_truck_month_value set depreciation_fee = ? where id is not null" ;
    var paramsArray=[],i=0;
    paramsArray[i++]=params.depreciationFee;
    if(params.truckId){
        paramsArray[i++] = params.truckId;
        query = query + " and truck_id = ? ";
    }
    if(params.yMonth){
        paramsArray[i++] = params.yMonth;
        query = query + " and y_month = ? ";
    }
    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateTruckDepreciationFee ');
        return callback(error,rows);
    });
}

function updateDepreciationFee(params,callback){
    var query = " update drive_truck_month_value set insure_fee = ? , depreciation_fee = ? where id = ? " ;
    var paramsArray=[],i=0;
    paramsArray[i++]=params.insureFee;
    paramsArray[i++]=params.depreciationFee;
    paramsArray[i++]=params.driveTruckMonthValueId;
    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateDepreciationFee ');
        return callback(error,rows);
    });
}

function deleteDriveTruckMonthValue(params,callback){
    var query = " delete from drive_truck_month_value where y_month = ? ";
    var paramsArray=[],i=0;
    paramsArray[i++] = params.yMonth;
    paramsArray[i++] = params.driveTruckMonthValueId;
    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' deleteDriveTruckMonthValue ');
        return callback(error,rows);
    });
}


module.exports ={
    addDistance : addDistance,
    updateStorageCarCount : updateStorageCarCount,
    updateOutput : updateOutput,
    updateInsureFee : updateInsureFee,
    updateDistanceSalary : updateDistanceSalary,
    updateDamage : updateDamage,
    updateCleanFee : updateCleanFee,
    updateHotelFee : updateHotelFee,
    updateEtcFee : updateEtcFee,
    updateOilFee : updateOilFee,
    updatePeccancy : updatePeccancy,
    updateRepair : updateRepair,
    updateCarOilFee : updateCarOilFee,
    updateTruckNum : updateTruckNum,
    updateDrive : updateDrive,
    getDriveTruckMonthValue : getDriveTruckMonthValue,
    updateTruckDepreciationFee : updateTruckDepreciationFee,
    updateDepreciationFee : updateDepreciationFee,
    deleteDriveTruckMonthValue : deleteDriveTruckMonthValue
}