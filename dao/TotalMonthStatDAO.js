/**
 * Created by yym on 2020/12/3.
 */

const db=require('../db/connection/MysqlDb.js');
const serverLogger = require('../util/ServerLogger.js');
const logger = serverLogger.createLogger('TotalMonthStatDAO.js');


function addTotalMonthStat(params,callback){
    var query = " insert into total_month_stat (y_month) " +
        " values ( ? ) ";
    var paramsArray=[],i=0;
    paramsArray[i++]=params.yMonth;
    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' addTotalMonthStat ');
        return callback(error,rows);
    });
}

//商品车数�?
function updateCarCount(params,callback) {
    var query = " UPDATE total_month_stat tms INNER JOIN( " +
        " SELECT count( ci.id ) car_count " +
        " FROM car_info ci " +
        " LEFT JOIN settle_car sc on ci.vin = sc.vin " +
        " WHERE ci.id is not null " +
        " AND ci.order_date_id >= " + params.yMonth + "01 " +
        " AND ci.order_date_id <= " + params.yMonth +"31 " +
        " AND ci.car_status = 9  ) cim " +
        " ON tms.y_month = " + params.yMonth  +
        " SET tms.car_count = cim.car_count";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateCarCount ');
        return callback(error,rows);
    });
}

//产�??
function updateOutputCount(params,callback) {
    var query = " UPDATE total_month_stat tms INNER JOIN( " +
        " SELECT sum(price+price_2+price_3) as sumFee " +
        " FROM car_info ci " +
        " LEFT JOIN settle_car sc on ci.vin = sc.vin " +
        " WHERE ci.id is not null " +
        " AND ci.order_date_id >= " + params.yMonth + "01 " +
        " AND ci.order_date_id <= " + params.yMonth +"31 " +
        " AND ci.car_status = 9   " +
        " AND ci.entrust_id = sc.entrust_id " +
        " AND ci.route_start_id = sc.route_start_id " +
        " AND ci.route_end_id = sc.route_end_id ) cim " +
        " ON tms.y_month = " + params.yMonth  +
        " SET tms.output = cim.sumFee";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateOutputCount ');
        return callback(error,rows);
    });
}

//运营货车数量 , 重载公里�? , 空载公里�? , 总公里数 , 重载�?
function updateTruckCount(params,callback) {
    var query = " UPDATE total_month_stat tms INNER JOIN( " +
        " SELECT sum(drt.truck_id) as truck_count, " +
        " sum( CASE WHEN drt.load_flag = 1 THEN drt.distance END ) AS load_distance, " +
        " sum( CASE WHEN drt.load_flag = 0 THEN drt.distance END ) AS no_load_distance " +
        " FROM dp_route_task drt " +
        " WHERE drt.id is not null " +
        " AND drt.date_id >= " + params.yMonth + "01 " +
        " AND drt.date_id <= " + params.yMonth +"31 " +
        " AND drt.task_status = 10 ) drtm " +
        " ON tms.y_month = " + params.yMonth  +
        " SET tms.truck_count = drtm.truck_count ," +
        " tms.load_distance = drtm.load_distance, " +
        " tms.no_load_distance =  drtm.no_load_distance, " +
        " tms.total_distance = drtm.load_distance + drtm.no_load_distance ," +
        " tms.load_ratio = drtm.load_distance/(drtm.load_distance + drtm.no_load_distance) ";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateTruckCount ');
        return callback(error,rows);
    });
}

/*外协商品车数�?1 , 外协费用1
结算直接查询*/
function updateOuterCarCount(params,callback) {
    var query = " UPDATE total_month_stat tms INNER JOIN( " +
        " SELECT sum( soicl.total_fee ) AS sumFee, " +
        " count( ci.id ) AS countCar " +
        " FROM car_info ci " +
        " LEFT JOIN settle_outer_invoice_car_rel soicl ON soicl.car_id = ci.id  " +
        " WHERE ci.id is not null " +
        " AND ci.order_date_id >= " + params.yMonth + "01 " +
        " AND ci.order_date_id <= " + params.yMonth + "31 " +
        " AND ci.car_status = 1 " +
        " AND ci.company_id > 0 ) cim " +
        " ON tms.y_month = " + params.yMonth  +
        " SET tms.outer_car_count = tms.outer_car_count + cim.countCar, " +
        " tms.outer_fee = tms.outer_fee + cim.sumFee";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateOuterCarCount ');
        return callback(error,rows);
    });
}

/*外协商品车数�?2 , 外协费用2
路线查询费用*/
function updateOuterRouteCarCount(params,callback) {
    var query = " UPDATE total_month_stat tms INNER JOIN( " +
        " SELECT sum( soicl.total_fee ) AS sumFee, " +
        " count( drt.id ) AS countCar  " +
        " FROM dp_route_task drt " +
        " LEFT JOIN dp_route_load_task_detail drltd ON drltd.dp_route_task_id = drt.id " +
        " LEFT JOIN car_info ci ON ci.id = drltd.car_id " +
        " LEFT JOIN settle_outer_invoice_car_rel soicl ON soicl.car_id = ci.id   " +
        " WHERE drt.id is not null " +
        " AND drt.date_id >= " + params.yMonth + "01 " +
        " AND drt.date_id <= " + params.yMonth + "31 " +
        " AND drt.outer_flag = 1 " +
        " AND drt.task_status = 10 ) drtm " +
        " ON tms.y_month = " + params.yMonth  +
        " SET tms.outer_car_count = tms.outer_car_count + drtm.countCar, " +
        " tms.outer_fee = tms.outer_fee + drtm.sumFee";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateOuterRouteCarCount ');
        return callback(error,rows);
    });
}

/*外协产�??1
结算直接查询*/
function updateOuterOutput(params,callback) {
    var query = " UPDATE total_month_stat tms INNER JOIN( " +
        " SELECT sum( sc.price + sc.price_2 + sc.price_3 ) AS sumPrice " +
        " FROM car_info ci " +
        " LEFT JOIN settle_car sc ON ci.vin = sc.vin  " +
        " WHERE ci.id is not null " +
        " AND ci.order_date_id >= " + params.yMonth + "01 " +
        " AND ci.order_date_id <= " + params.yMonth + "31 " +
        " AND ci.car_status = 1 " +
        " AND ci.company_id > 0 " +
        " AND ci.entrust_id = sc.entrust_id " +
        " AND ci.route_start_id = sc.route_start_id " +
        " AND ci.route_end_id = sc.route_end_id  ) cim " +
        " ON tms.y_month = " + params.yMonth  +
        " SET tms.outer_output = tms.outer_output + cim.sumPrice ";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateOuterOutput ');
        return callback(error,rows);
    });
}

/*外协产�??2
路线查询费用*/
function updateOuterRouteOutput(params,callback) {
    var query = " UPDATE total_month_stat tms INNER JOIN( " +
        " SELECT sum( sc.price + sc.price_2 + sc.price_3 ) AS sumPrice " +
        " FROM dp_route_task drt " +
        " LEFT JOIN dp_route_load_task_detail drltd ON drltd.dp_route_task_id = drt.id " +
        " LEFT JOIN car_info ci ON ci.id = drltd.car_id " +
        " LEFT JOIN settle_car sc ON ci.vin = sc.vin  " +
        " WHERE drt.id is not null " +
        " AND drt.date_id >= " + params.yMonth + "01 " +
        " AND drt.date_id <= " + params.yMonth + "31 " +
        " AND drt.outer_flag = 1 " +
        " AND drt.task_status = 10 " +
        " AND ci.entrust_id = sc.entrust_id " +
        " AND ci.route_start_id = sc.route_start_id " +
        " AND ci.route_end_id = sc.route_end_id  ) drtm " +
        " ON tms.y_month = " + params.yMonth  +
        " SET tms.outer_output = tms.outer_output + drtm.sumPrice";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateOuterRouteOutput ');
        return callback(error,rows);
    });
}

//过路�?
function updateEtcFeeCount(params,callback) {
    var query = " UPDATE total_month_stat tms INNER JOIN( " +
        " SELECT sum(te.etc_fee) AS sumFee " +
        " FROM truck_etc te " +
        " WHERE te.id is not null " +
        " AND te.date_id >= " + params.yMonth + "01 " +
        " AND te.date_id <= " + params.yMonth +"31 ) tem " +
        " ON tms.y_month = " + params.yMonth  +
        " SET tms.etc_fee = tem.sumFee ";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateEtcFeeCount ');
        return callback(error,rows);
    });
}

//加油�? , 加油�? , 尿素�? , 尿素�?
function updateOilCount(params,callback) {
    var query = " UPDATE total_month_stat tms INNER JOIN( " +
        " SELECT sum(deor.oil) as oil, " +
        " sum(deor.oil_money) as oil_money, " +
        " sum(deor.urea) as urea, " +
        " sum(deor.urea_money) as urea_money " +
        " FROM drive_exceed_oil_rel deor  " +
        " WHERE deor.id is not null " +
        " AND deor.date_id >= " + params.yMonth + "01 " +
        " AND deor.date_id <= " + params.yMonth +"31 ) deorm " +
        " ON tms.y_month = " + params.yMonth  +
        " SET tms.oil_vol = deorm.oil, " +
        " tms.oil_fee = deorm.oil_money, " +
        " tms.urea_vol = deorm.urea, " +
        " tms.urea_fee = deorm.urea_money ";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateOilCount ');
        return callback(error,rows);
    });
}

//修车�? , 零件�? , 保养�?
//内部维修�? , 内部维修�? , 在外维修次数 , 在外维修�?
function updateRepairCount(params,callback) {
    var query = " UPDATE total_month_stat tms INNER JOIN( " +
        " SELECT sum( trr.repair_money ) as repari_fee, " +
        " sum( trr.parts_money ) as part_fee, " +
        " sum( trr.maintain_money ) as maintain_fee ," +
        " count( CASE WHEN trr.repair_type = 2 THEN trr.repair_money END ) AS inner_repair_count, " +
        " count( CASE WHEN trr.repair_type = 3 THEN trr.repair_money END ) AS outer_repair_count, " +
        " sum( CASE WHEN trr.repair_type = 2 THEN trr.repair_money END ) AS inner_repari_fee, " +
        " sum( CASE WHEN trr.repair_type = 3 THEN trr.repair_money END ) AS outer_repair_fee " +
        " FROM truck_repair_rel trr " +
        " WHERE trr.id is not null " +
        " AND trr.date_id >= " + params.yMonth + "01 " +
        " AND trr.date_id <= " + params.yMonth +"31 " +
        " AND trr.payment_status = 1 " +
        " AND trr.repair_status = 1 ) trrm " +
        " ON tms.y_month = " + params.yMonth  +
        " SET tms.repari_fee = trrm.repari_fee, " +
        " tms.part_fee = trrm.part_fee, " +
        " tms.maintain_fee = trrm.maintain_fee, " +
        " tms.inner_repair_count = trrm.inner_repair_count, " +
        " tms.inner_repari_fee = trrm.inner_repari_fee, " +
        " tms.outer_repair_count = trrm.outer_repair_count," +
        " tms.outer_repair_fee = trrm.outer_repair_fee";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateRepairCount ');
        return callback(error,rows);
    });
}

//处罚次数 , 处罚分数 , 买分金额 , 交�?�罚�? ,
//处理金额 , 司机承担罚款 , 公司承担
function updatePeccancyCount(params,callback) {
    var query = " UPDATE total_month_stat tms INNER JOIN( " +
        " SELECT count( dp.fine_score ) AS fine_count, " +
        " sum( dp.fine_score ) AS fine_score, " +
        " sum( dp.buy_score ) AS buy_score_fee, " +
        " sum( dp.traffic_fine ) AS traffic_fine_fee, " +
        " sum( dp.fine_money ) AS fine_money, " +
        " sum( dp.under_money ) AS driver_under_money, " +
        " sum( dp.company_money ) AS company_under_money  " +
        " FROM drive_peccancy dp " +
        " WHERE dp.id is not null " +
        " AND dp.handle_date >= " + params.yMonth + "01 " +
        " AND dp.handle_date <= " + params.yMonth +"31 ) dpm " +
        " ON tms.y_month = " + params.yMonth  +
        " SET tms.fine_count = dpm.fine_count, " +
        " tms.fine_score = dpm.fine_score, " +
        " tms.buy_score_fee = dpm.buy_score_fee, " +
        " tms.traffic_fine_fee = dpm.traffic_fine_fee, " +
        " tms.fine_money = dpm.fine_money, " +
        " tms.driver_under_money = dpm.driver_under_money, " +
        " tms.company_under_money = dpm.company_under_money ";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updatePeccancyCount ');
        return callback(error,rows);
    });
}

//质损�? , 个人承担质损�? , 公司承担质损�? , 质损总成�?
function updateDamageCount(params,callback) {
    var query = " UPDATE total_month_stat tms INNER JOIN( " +
        " SELECT count(di.id) as damage_count, " +
        " sum(under_cost) as person_damage_money, " +
        " sum(company_cost) as company_damage_money  " +
        " FROM damage_info di " +
        " LEFT JOIN damage_check dc on dc.damage_id = di.id " +
        " WHERE di.id is not null " +
        " AND dc.date_id >= " + params.yMonth + "01 " +
        " AND dc.date_id <= " + params.yMonth +"31 " +
        " AND di.damage_status = 3 ) dim " +
        " ON tms.y_month = " + params.yMonth  +
        " SET tms.damage_count = dim.damage_count, " +
        " tms.person_damage_money = dim.person_damage_money, " +
        " tms.company_damage_money = dim.company_damage_money, " +
        " tms.total_damange_money = dim.person_damage_money + dim.company_damage_money ";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateDamageCount ');
        return callback(error,rows);
    });
}

//商品车保险待�?
// function updateInsureCount(params,callback) {
//     var query = " UPDATE total_month_stat tms INNER JOIN( " +
//         " SELECT sum( insure_actual ) AS car_insurance " +
//         " FROM damage_insure di " +
//         " WHERE di.id is not null " +
//         " AND di.declare_date >= " + params.yMonth + "01 " +
//         " AND di.declare_date <= " + params.yMonth +"31 " +
//         " AND di.insure_status = 1 ) dim " +
//         " ON tms.y_month = " + params.yMonth  +
//         " SET tms.car_insurance = dim.car_insurance ";
//     var paramsArray=[],i=0;
//
//     db.dbQuery(query,paramsArray,function(error,rows){
//         logger.debug(' updateInsureCount ');
//         return callback(error,rows);
//     });
// }

//洗车�?
function updateCleanFeeCount(params,callback) {
    var query = " UPDATE total_month_stat tms INNER JOIN( " +
        " SELECT sum( drlt.total_price ) AS clean_fee " +
        " FROM dp_route_load_task_clean_rel drlt " +
        " WHERE drlt.id is not null " +
        " AND drlt.date_id >= " + params.yMonth + "01 " +
        " AND drlt.date_id <= " + params.yMonth +"31 " +
        " AND drlt.status =2 ) dim " +
        " ON tms.y_month = " + params.yMonth  +
        " SET tms.clean_fee = dim.clean_fee ";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateCleanFeeCount ');
        return callback(error,rows);
    });
}

//进门�? , 拖车�? , 商品车停车费 , 地跑�? , 带路�?
function updateDriveTruckFeeCount(params,callback) {
    var query = " UPDATE total_month_stat tms INNER JOIN( " +
        " SELECT sum( dtmv.enter_fee ) AS enter_fee, " +
        " sum( dtmv.trailer_fee ) AS trail_fee, " +
        " sum( dtmv.car_parking_total_fee ) AS car_parking_fee, " +
        " sum( dtmv.run_fee ) AS run_fee, " +
        " sum( dtmv.lead_fee ) AS lead_fee " +
        " FROM drive_truck_month_value dtmv " +
        " WHERE dtmv.id is not null " +
        " AND dtmv.y_month = " + params.yMonth + " ) dtmvm " +
        " ON tms.y_month = " + params.yMonth  +
        " SET tms.enter_fee = dtmvm.enter_fee, " +
        " tms.trail_fee = dtmvm.trail_fee, " +
        " tms.car_parking_fee = dtmvm.car_parking_fee, " +
        " tms.run_fee =dtmvm.run_fee, " +
        " tms.lead_fee = dtmvm.lead_fee ";
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateDriveTruckFeeCount ');
        return callback(error,rows);
    });
}

//单车产�?? , 单公里产�?
function updatePerOutputCount(params,callback) {
    var query = " UPDATE total_month_stat " +
        " SET per_truck_output = output / car_count, " +
        " per_km_output = output / total_distance " +
        " WHERE y_month = " + params.yMonth ;
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updatePerOutputCount ');
        return callback(error,rows);
    });
}

//单车质损成本 , 单车公司承担成本
function updatePerCarDamageMoneyCount(params,callback) {
    var query = " UPDATE total_month_stat " +
        " SET per_car_damage_money = total_damange_money / car_count, " +
        " per_car_c_damange_money = company_damage_money / car_count " +
        " WHERE y_month = " + params.yMonth ;
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updatePerCarDamageMoneyCount ');
        return callback(error,rows);
    });
}

//单车洗车�?
function updatePerCarCleanFeeCount(params,callback) {
    var query = " UPDATE total_month_stat " +
        " SET per_car_clean_fee = clean_fee / car_count " +
        " WHERE y_month = " + params.yMonth ;
    var paramsArray=[],i=0;

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updatePerCarCleanFeeCount ');
        return callback(error,rows);
    });
}


function deleteTotalMonthStat(params,callback){
    var query = " delete from total_month_stat where y_month = ? ";
    var paramsArray=[],i=0;
    paramsArray[i++] = params.yMonth;
    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' deleteTotalMonthStat ');
        return callback(error,rows);
    });
}


module.exports ={
    addTotalMonthStat : addTotalMonthStat,
    updateCarCount : updateCarCount,
    updateOutputCount : updateOutputCount,
    updateTruckCount : updateTruckCount,
    updateOuterCarCount : updateOuterCarCount,
    updateOuterRouteCarCount : updateOuterRouteCarCount,
    updateOuterOutput : updateOuterOutput,
    updateOuterRouteOutput : updateOuterRouteOutput,
    updateEtcFeeCount : updateEtcFeeCount,
    updateOilCount : updateOilCount,
    updateRepairCount : updateRepairCount,
    updatePeccancyCount : updatePeccancyCount,
    updateDamageCount : updateDamageCount,
    updateCleanFeeCount : updateCleanFeeCount,
    updateDriveTruckFeeCount : updateDriveTruckFeeCount,
    updatePerOutputCount : updatePerOutputCount,
    updatePerCarDamageMoneyCount : updatePerCarDamageMoneyCount,
    updatePerCarCleanFeeCount : updatePerCarCleanFeeCount,
    deleteTotalMonthStat : deleteTotalMonthStat
}