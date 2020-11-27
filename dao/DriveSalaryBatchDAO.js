
const db=require('../db/connection/MysqlDb.js');
const serverLogger = require('../util/ServerLogger.js');
var logger = serverLogger.createLogger('DriveSalaryBatchDAO.js');

function addDriveSalaryBatch(params, callback) {
    // 默认插入字段：月份, 司机ID, 公司ID， 用户ID
    var query = "INSERT INTO drive_salary(month_date_id, drive_id, company_id, user_id)" +
        // 洗车费相关 + 杂费相关 + 暂扣款
        " SELECT " + params.yMonth + " as month_date_id, dtt.drive_id, di.company_id, di.user_id" +
        " FROM ( " +
        "    SELECT DISTINCT drive_id FROM dp_route_task WHERE task_plan_date>='" + params.monthStart + "' AND task_plan_date<='" + params.monthEnd + "' AND task_status=10" +
        "    UNION SELECT DISTINCT drive_id FROM drive_sundry_fee WHERE y_month=" + params.yMonth +
        "    UNION SELECT DISTINCT drive_id FROM drive_salary_retain WHERE y_month=" + params.yMonth +
        "    UNION SELECT DISTINCT drive_id FROM drive_work WHERE y_month=" + params.yMonth +
        "    UNION SELECT DISTINCT drive_id FROM drive_peccancy WHERE date_id>=" + params.yMonth + "01 AND date_id<=" + params.yMonth + "31) as dtt" +
        " LEFT JOIN drive_info di ON di.id = dtt.drive_id" +
        // 费用申请
        " UNION" +
        " SELECT " + params.yMonth + " as month_date_id, di.id as drive_id, di.company_id, di.user_id " +
        " FROM dp_route_task_fee drtf" +
        " LEFT JOIN drive_info di on drtf.drive_id = di.id" +
        " WHERE drtf.drive_id is not null AND drtf.created_on>='" + params.monthStart + " 00:00:00' AND drtf.created_on<='" + params.monthEnd + " 23:59:59' AND drtf.status=2" +
        // 商品车质损相关
        " UNION" +
        " SELECT " + params.yMonth + " as month_date_id, di.id as drive_id, di.company_id, di.user_id " +
        " FROM damage_check dc" +
        " LEFT JOIN drive_info di on dc.under_user_id = di.user_id" +
        " LEFT JOIN damage_info dai on dc.damage_id = dai.id" +
        " WHERE di.id is not null AND dai.date_id>=" + params.yMonth + "01 AND dai.date_id<=" + params.yMonth + "31 AND dai.damage_status =3 " +
        // 货车事故承担相关
        " UNION" +
        " SELECT " + params.yMonth + " as month_date_id, di.id as drive_id, di.company_id, di.user_id " +
        " FROM truck_accident_check tac" +
        " LEFT JOIN drive_info di on tac.under_user_id = di.user_id " +
        " LEFT JOIN truck_accident_info tai on tac.truck_accident_id = tai.id " +
        " WHERE di.id is not null AND tai.date_id>=" + params.yMonth + "01 AND tai.date_id<=" + params.yMonth + "31 AND tai.accident_status =3 ";
    var paramsArray = [], i = 0;
    db.dbQuery(query, paramsArray, function (error, rows) {
        logger.debug(' addDriveSalaryBatch ');
        return callback(error, rows);
    });
}

// 商品车质损
function updateDamageUnderFee(params, callback) {
    var query = "UPDATE drive_salary as ds" +
        " INNER JOIN (" +
        "   SELECT di.id as drive_id, sum(dc.under_cost) as damage_under_fee" +
        "   FROM damage_check dc" +
        "   LEFT JOIN drive_info di on dc.under_user_id = di.user_id" +
        "   WHERE di.id is not null AND dc.date_id>=" + params.yMonth + "01 AND dc.date_id<=" + params.yMonth + "31" +
        "   GROUP BY di.id ) as base" +
        " ON ds.drive_id = base.drive_id " +
        " AND ds.month_date_id = " + params.yMonth +
        // 更新字段
        " SET ds.damage_under_fee=base.damage_under_fee";
    var paramsArray = [], i = 0;
    db.dbQuery(query, paramsArray, function (error, rows) {
        logger.debug(' updateDamageUnderFee ');
        return callback(error, rows);
    });
}

// 货车事故承担
function updateAccidentFee(params, callback) {
    var query = "UPDATE drive_salary as ds" +
        " INNER JOIN (" +
        "   SELECT di.id as drive_id ,sum(tc.under_cost) as accident_fee" +
        "   FROM truck_accident_check tc" +
        "   LEFT JOIN drive_info di on tc.under_user_id = di.user_id" +
        "   WHERE di.id is not null AND tc.date_id>=" + params.yMonth + "01 AND tc.date_id<=" + params.yMonth + "31" +
        "   GROUP BY di.id ) as base" +
        " ON ds.drive_id = base.drive_id " +
        " AND ds.month_date_id = " + params.yMonth +
        // 更新字段
        " SET ds.accident_fee=base.accident_fee";
    var paramsArray = [], i = 0;
    db.dbQuery(query, paramsArray, function (error, rows) {
        logger.debug(' updateAccidentFee ');
        return callback(error, rows);
    });
}

// 违章扣款
function updatePeccancyUnderFee(params, callback) {
    var query = "UPDATE drive_salary as ds" +
        " INNER JOIN (" +
        "   SELECT drive_id, sum(under_money) as peccancy_under_fee" +
        "   FROM drive_peccancy" +
        "   WHERE date_id>=" + params.yMonth + "01 AND date_id<=" + params.yMonth + "31" +
        "   GROUP BY drive_id ) as base" +
        " ON ds.drive_id = base.drive_id " +
        " AND ds.month_date_id = " + params.yMonth +
        // 更新字段
        " SET ds.peccancy_under_fee=base.peccancy_under_fee";
    var paramsArray = [], i = 0;
    db.dbQuery(query, paramsArray, function (error, rows) {
        logger.debug(' updatePeccancyUnderFee ');
        return callback(error, rows);
    });
}

// 超量扣款
function updateExceedOilFee(params, callback) {
    var query = "UPDATE drive_salary as ds" +
        " INNER JOIN (" +
        "   SELECT drive_id, sum(gps_actual_money) as exceed_oil_fee" +
        "   FROM drive_exceed_oil_date" +
        "   WHERE month_date_id=" + params.yMonth + " AND check_status=3" +
        "   GROUP BY drive_id ) as base" +
        " ON ds.drive_id = base.drive_id " +
        " AND ds.month_date_id = " + params.yMonth +
        // 更新字段
        " SET ds.exceed_oil_fee=base.exceed_oil_fee";
    var paramsArray = [], i = 0;
    db.dbQuery(query, paramsArray, function (error, rows) {
        logger.debug(' updateExceedOilFee ');
        return callback(error, rows);
    });
}

// 满勤补助，出差补助，其他补助
function updateBonus(params, callback) {
    var query = "UPDATE drive_salary as ds" +
        " INNER JOIN (" +
        "   SELECT drive_id, sum(full_work_bonus) as full_work_bonus, sum(hotel_bonus) as hotel_bonus, sum(other_bonus) as other_bonus" +
        "   FROM drive_work" +
        "   WHERE y_month=" + params.yMonth +
        "   GROUP BY drive_id ) as base" +
        " ON ds.drive_id = base.drive_id " +
        " AND ds.month_date_id = " + params.yMonth +
        // 更新字段
        " SET ds.full_work_bonus=base.full_work_bonus," +
        "     ds.hotel_bonus=base.hotel_bonus," +
        "     ds.other_bonus=base.other_bonus";
    var paramsArray = [], i = 0;
    db.dbQuery(query, paramsArray, function (error, rows) {
        logger.debug(' updateBonus ');
        return callback(error, rows);
    });
}

// 社保缴费，伙食费，个人借款，其他扣款
function updateSundryFee(params, callback) {
    var query = "UPDATE drive_salary as ds" +
        " INNER JOIN (" +
        "   SELECT drive_id, sum(social_fee) as social_security_fee, sum(meals_fee) as food_fee, sum(personal_loan) as loan_fee, sum(other_fee) as other_fee" +
        "   FROM drive_sundry_fee" +
        "   WHERE y_month=" + params.yMonth +
        "   GROUP BY drive_id ) as base" +
        " ON ds.drive_id = base.drive_id " +
        " AND ds.month_date_id = " + params.yMonth +
        // 更新字段
        " SET ds.social_security_fee=base.social_security_fee," +
        "     ds.food_fee=base.food_fee," +
        "     ds.loan_fee=base.loan_fee," +
        "     ds.other_fee=base.other_fee";
    var paramsArray = [], i = 0;
    db.dbQuery(query, paramsArray, function (error, rows) {
        logger.debug(' updateSundryFee ');
        return callback(error, rows);
    });
}

// 质损暂扣款，质安罚款，交车暂扣款
function updateRetainFfee(params, callback) {
    var query = "UPDATE drive_salary as ds" +
        " INNER JOIN (" +
        "   SELECT drive_id, sum(damage_retain_fee) as damage_retain_fee, sum(damage_op_fee) as damage_op_fee, sum(truck_retain_fee) as truck_retain_fee" +
        "   FROM drive_salary_retain" +
        "   WHERE y_month= " + params.yMonth +
        "   GROUP BY drive_id ) as base" +
        " ON ds.drive_id = base.drive_id " +
        " AND ds.month_date_id = " + params.yMonth +
        // 更新字段
        " SET ds.damage_retain_fee=base.damage_retain_fee," +
        "     ds.damage_op_fee=base.damage_op_fee," +
        "     ds.truck_retain_fee=base.truck_retain_fee";
    var paramsArray = [], i = 0;
    db.dbQuery(query, paramsArray, function (error, rows) {
        logger.debug(' updateRetainFfee ');
        return callback(error, rows);
    });
}

// 费用申请：商品车加油费，货车停车费，商品车停车费，其它运送费用
function updateDpRouteTaskFee(params, callback) {
    var query = "UPDATE drive_salary as ds" +
        " INNER JOIN (" +
        "   SELECT drive_id, sum(car_oil_fee) as car_oil_fee, sum(total_price) as truck_parking_fee, sum(car_total_price) as car_parking_fee, sum(other_fee) as dp_other_fee" +
        "   FROM dp_route_task_fee" +
        "   WHERE created_on>='" + params.monthStart + " 00:00:00 ' AND created_on<='" + params.monthEnd + " 23:59:59' AND status=2" +
        "   GROUP BY drive_id ) as base" +
        " ON ds.drive_id = base.drive_id " +
        " AND ds.month_date_id = " + params.yMonth +
        // 更新字段
        " SET ds.car_oil_fee=base.car_oil_fee," +
        "     ds.truck_parking_fee=base.truck_parking_fee," +
        "     ds.car_parking_fee=base.car_parking_fee," +
        "     ds.dp_other_fee=base.dp_other_fee";
    var paramsArray = [], i = 0;
    db.dbQuery(query, paramsArray, function (error, rows) {
        logger.debug(' updateDpRouteTaskFee ');
        return callback(error, rows);
    });
}

// 洗车费相关：应发洗车费，应发拖车费，应发地跑费，应发带路费，提车费
function updateCleanFee(params, callback) {
    var query = "UPDATE drive_salary as ds" +
        " INNER JOIN (" +
        "   SELECT dpltcr.drive_id, sum(dpltcr.actual_price) as clean_fee, sum(dpltcr.total_trailer_fee) as trailer_fee, sum(dpltcr.total_run_fee) as run_fee, sum(dpltcr.lead_fee) as lead_fee, sum(dpltcr.car_parking_fee) as car_pick_fee" +
        "   FROM dp_route_load_task_clean_rel dpltcr" +
        "   LEFT JOIN dp_route_load_task dplt ON dplt.id = dpltcr.dp_route_load_task_id" +
        "   WHERE dplt.load_date >= '" + params.monthStart + " 00:00:00' AND dplt.load_date <= '" + params.monthEnd + "23:59:59' AND dpltcr.status=2" +
        "   GROUP BY dpltcr.drive_id) as base" +
        " ON ds.drive_id = base.drive_id " +
        " AND ds.month_date_id = " + params.yMonth +
        // 更新字段
        " SET ds.clean_fee=base.clean_fee," +
        "     ds.trailer_fee=base.trailer_fee," +
        "     ds.run_fee=base.run_fee," +
        "     ds.lead_fee=base.lead_fee," +
        "     ds.car_pick_fee=base.car_pick_fee";
    var paramsArray = [], i = 0;
    db.dbQuery(query, paramsArray, function (error, rows) {
        logger.debug(' updateCleanFee ');
        return callback(error, rows);
    });
}

// 里程工资，倒板工资
function updateDistanceSalary(params, callback) {
    var query = "UPDATE drive_salary as ds" +
        " INNER JOIN (" +
        "   SELECT drive_id, sum(CASE" +
        "      WHEN reverse_flag=0 and truck_number=6 and car_count<=3 then distance*0.6" +
        "      WHEN reverse_flag=0 and truck_number=6 and car_count=4 then distance*0.7" +
        "      WHEN reverse_flag=0 and truck_number=6 and car_count=5 then distance*0.8" +
        "      WHEN reverse_flag=0 and truck_number=6 and car_count=6 then distance*0.9" +
        "      WHEN reverse_flag=0 and truck_number=6 and car_count>=7 then distance" +
        "      WHEN reverse_flag=0 and truck_number=8 and car_count<5 then distance*0.6" +
        "      WHEN reverse_flag=0 and truck_number=8 and car_count=5 then distance*0.7" +
        "      WHEN reverse_flag=0 and truck_number=8 and car_count=6 then distance*0.8" +
        "      WHEN reverse_flag=0 and truck_number=8 and car_count=7 then distance*0.9" +
        "      WHEN reverse_flag=0 and truck_number=8 and car_count=8 then distance" +
        "      WHEN reverse_flag=0 and truck_number=8 and car_count>=9 then distance*1.4" +
        "      ELSE '0' END) distance_salary," +
        "      sum(CASE WHEN reverse_flag=1 then reverse_money ELSE '0' END) reverse_salary" +
        "   FROM dp_route_task " +
        "   WHERE task_plan_date>='" + params.monthStart + "' and task_plan_date<='" + params.monthEnd + "' and task_status>=9 " +
        "   GROUP BY drive_id) as base" +
        " ON ds.drive_id = base.drive_id" +
        " AND ds.month_date_id = " + params.yMonth +
        // 更新字段
        " SET ds.distance_salary=base.distance_salary," +
        "     ds.reverse_salary=base.reverse_salary";
    var paramsArray = [], i = 0;
    db.dbQuery(query, paramsArray, function (error, rows) {
        logger.debug(' updateDistanceAndReverseSalary ');
        return callback(error, rows);
    });
}

// 交车打车进门费
function updateEnterFee(params, callback) {
    var query = "UPDATE drive_salary as ds" +
        " INNER JOIN (" +
        "   SELECT dpr.drive_id, sum(CASE WHEN dprl.receive_flag=0 and dprl.transfer_flag=0 THEN dprl.real_count ELSE '0' END)*4 as enter_fee" +
        "   FROM dp_route_load_task dprl" +
        "   LEFT JOIN dp_route_task dpr on dprl.dp_route_task_id = dpr.id" +
        "   WHERE dpr.task_plan_date>='" + params.monthStart + "' AND dpr.task_plan_date<='" + params.monthEnd + "' AND dpr.task_status>=9" +
        "   GROUP BY dpr.drive_id) as base" +
        " ON ds.drive_id = base.drive_id " +
        " AND ds.month_date_id = " + params.yMonth +
        // 更新字段
        " SET ds.enter_fee=base.enter_fee";
    var paramsArray = [], i = 0;
    db.dbQuery(query, paramsArray, function (error, rows) {
        logger.debug(' updateEnterFee ');
        return callback(error, rows);
    });
}

// 重载，空载
function updateLoadDistance(params, callback) {
    var query = "UPDATE drive_salary as ds" +
        " INNER JOIN (" +
        "   SELECT drive_id," +
        "   sum(CASE WHEN load_flag = 1 THEN distance ELSE '0' END) as load_distance," +
        "   sum(CASE WHEN load_flag = 0 THEN distance ELSE '0' END) as no_load_distance" +
        "   FROM dp_route_task" +
        "   WHERE task_plan_date>='" + params.monthStart + "' AND task_plan_date<='" + params.monthEnd + "' AND task_status>=9" +
        "   GROUP BY drive_id) as base" +
        " ON ds.drive_id = base.drive_id" +
        " AND ds.month_date_id = " + params.yMonth +
        // 更新字段
        " SET ds.load_distance=base.load_distance," +
        "     ds.no_load_distance=base.no_load_distance";
    var paramsArray = [], i = 0;
    db.dbQuery(query, paramsArray, function (error, rows) {
        logger.debug(' updateLoadDistance ');
        return callback(error, rows);
    });
}

// 删除指定月 所有数据
function deleteDriveSalary(params,callback){
    var query = "DELETE FROM drive_salary WHERE month_date_id = ? ";
    var paramsArray=[],i=0;
    paramsArray[i++] = params.yMonth;
    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' deleteDriveSalary ');
        return callback(error,rows);
    });
}

function updateDriveSalaryPersonalTax(params,callback){
    var query = " update drive_salary set  " +
        "actual_salary = IFNULL(distance_salary + reverse_salary + enter_fee " +
        " - damage_under_fee - accident_fee - peccancy_under_fee - exceed_oil_fee " +
        " + full_work_bonus + other_bonus " +
        " - hotel_bonus - social_security_fee - food_fee - loan_fee " +
        " - other_fee - damage_retain_fee - damage_op_fee - truck_retain_fee " +
        " + car_oil_fee + truck_parking_fee + car_parking_fee + dp_other_fee " +
        " + clean_fee  + trailer_fee + run_fee + lead_fee + car_pick_fee " +
        " - personal_tax,0) " +
        " where id is not null ";
    var paramsArray=[],i=0;

    if(params.yMonth){
        paramsArray[i] = params.yMonth;
        query = query + " and month_date_id = ? ";
    }

    db.dbQuery(query,paramsArray,function(error,rows){
        logger.debug(' updateDriveSalaryPersonalTax ');
        return callback(error,rows);
    });
}

module.exports = {
    addDriveSalaryBatch: addDriveSalaryBatch,
    updateDamageUnderFee: updateDamageUnderFee,
    updateAccidentFee: updateAccidentFee,
    updatePeccancyUnderFee: updatePeccancyUnderFee,
    updateExceedOilFee: updateExceedOilFee,
    updateBonus: updateBonus,
    updateSundryFee: updateSundryFee,
    updateRetainFfee: updateRetainFfee,
    updateDpRouteTaskFee: updateDpRouteTaskFee,
    updateCleanFee: updateCleanFee,
    updateDistanceSalary: updateDistanceSalary,
    updateEnterFee: updateEnterFee,
    updateLoadDistance: updateLoadDistance,
    deleteDriveSalary: deleteDriveSalary ,
    updateDriveSalaryPersonalTax: updateDriveSalaryPersonalTax
};