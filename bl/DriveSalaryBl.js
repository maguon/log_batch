var sysMsg = require('../util/SystemMsg.js');
var sysError = require('../util/SystemError.js');
var resUtil = require('../util/ResponseUtil.js');
var driveSalaryBatchDAO = require('../dao/DriveSalaryBatchDAO.js');

var Seq = require('seq');
var serverLogger = require('../util/ServerLogger.js');
var logger = serverLogger.createLogger('DriveSalary.js');
var moment = require('moment/moment.js');

// 批量插入工资数据 按年月
function createDriveSalaryBatch(req,res,next){
    var params = {} ;
    var myDate = new Date();
    var yMonthDay = new Date(myDate-30*24*60*60*1000);
    var yMonth = moment(yMonthDay).format('YYYYMM');
    params.yMonth = yMonth;
    params.monthStart = params.yMonth.substr(0,4) + '-' + params.yMonth.substr(4,2) + '-01';
    params.monthEnd = params.yMonth.substr(0,4) + '-' + params.yMonth.substr(4,2) + '-31';
    Seq().seq(function(){
        var that = this;
        // 删除 司机工资表 指定月份数据
        driveSalaryBatchDAO.deleteDriveSalary(params,function(error,result){
            if (error) {
                logger.error(' deleteDriveSalary ' + error.message);
                throw sysError.InternalError(error.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' deleteDriveSalary ' + 'success');
                }else{
                    logger.warn(' deleteDriveSalary ' + 'failed');
                }
                that();
            }
        })
    }).seq(function(){
        var that = this;
        // 创建 司机工资表 基础数据：月份, 司机ID, 公司ID， 用户ID
        driveSalaryBatchDAO.addDriveSalaryBatch(params,function(error,result){
            if (error) {
                if(error.message.indexOf("Duplicate") > 0) {
                    resUtil.resetFailedRes(res, "数据已经存在");
                    return next();
                } else{
                    logger.error(' addDriveSalaryBatch ' + error.message);
                    throw sysError.InternalError(error.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
                }
            } else {
                if(result&&result.insertId>0){
                    logger.info(' addDriveSalaryBatch ' + 'success');
                }else{
                    logger.warn(' addDriveSalaryBatch ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        // 更新 商品车质损
        driveSalaryBatchDAO.updateDamageUnderFee(params,function(err,result){
            if (err) {
                logger.error(' updateDamageUnderFee ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateDamageUnderFee ' + 'success');
                }else{
                    logger.warn(' updateDamageUnderFee ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        // 更新 货车事故承担
        driveSalaryBatchDAO.updateAccidentFee(params,function(err,result){
            if (err) {
                logger.error(' updateAccidentFee ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateAccidentFee ' + 'success');
                }else{
                    logger.warn(' updateAccidentFee ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        // 更新 违章扣款
        driveSalaryBatchDAO.updatePeccancyUnderFee(params,function(err,result){
            if (err) {
                logger.error(' updatePeccancyUnderFee ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updatePeccancyUnderFee ' + 'success');
                }else{
                    logger.warn(' updatePeccancyUnderFee ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        // 更新 超量扣款
        driveSalaryBatchDAO.updateExceedOilFee(params,function(err,result){
            if (err) {
                logger.error(' updateExceedOilFee ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateExceedOilFee ' + 'success');
                }else{
                    logger.warn(' updateExceedOilFee ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        // 更新 满勤补助，出差补助，其他补助
        driveSalaryBatchDAO.updateBonus(params,function(err,result){
            if (err) {
                logger.error(' updateBonus ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateBonus ' + 'success');
                }else{
                    logger.warn(' updateBonus ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        // 更新 社保缴费，伙食费，个人借款，其他扣款
        driveSalaryBatchDAO.updateSundryFee(params,function(err,result){
            if (err) {
                logger.error(' updateSundryFee ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateSundryFee ' + 'success');
                }else{
                    logger.warn(' updateSundryFee ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        // 更新 质损暂扣款，质安罚款，交车暂扣款
        driveSalaryBatchDAO.updateRetainFfee(params,function(err,result){
            if (err) {
                logger.error(' updateRetainFfee ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateRetainFfee ' + 'success');
                }else{
                    logger.warn(' updateRetainFfee ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        // 更新 费用申请：商品车加油费，货车停车费，商品车停车费，其它运送费用
        driveSalaryBatchDAO.updateDpRouteTaskFee(params,function(err,result){
            if (err) {
                logger.error(' updateDpRouteTaskFee ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateDpRouteTaskFee ' + 'success');
                }else{
                    logger.warn(' updateDpRouteTaskFee ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        // 更新 洗车费相关：应发洗车费，应发拖车费，应发地跑费，应发带路费，提车费
        driveSalaryBatchDAO.updateCleanFee(params,function(err,result){
            if (err) {
                logger.error(' updateCleanFee ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateCleanFee ' + 'success');
                }else{
                    logger.warn(' updateCleanFee ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        // 更新 里程工资，倒板工资
        driveSalaryBatchDAO.updateDistanceSalary(params,function(err,result){
            if (err) {
                logger.error(' updateDistanceSalary ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateDistanceSalary ' + 'success');
                }else{
                    logger.warn(' updateDistanceSalary ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        // 更新 交车打车进门费
        driveSalaryBatchDAO.updateEnterFee(params,function(err,result){
            if (err) {
                logger.error(' updateEnterFee ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateEnterFee ' + 'success');
                }else{
                    logger.warn(' updateEnterFee ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that= this;
        // 更新 重载，空载
        driveSalaryBatchDAO.updateLoadDistance(params,function(err,result){
            if (err) {
                logger.error(' updateLoadDistance ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                logger.info(' updateLoadDistance ' + 'success');
                that();
            }
        })
    }).seq(function () {
        var that = this;
        // 更新 系数
        driveSalaryBatchDAO.updateSalaryRatio(params,function(err,result){
            if (err) {
                logger.error(' updateSalaryRatio ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateSalaryRatio ' + 'success');
                }else{
                    logger.warn(' updateSalaryRatio ' + 'failed');
                }
                that();
            }
        })
    }).seq(function(){
        driveSalaryBatchDAO.updateDriveSalaryPersonalTax(params,function(err,result){
            if (err) {
                logger.error(' updateDriveSalaryPersonalTax ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                logger.info(' updateDriveSalaryPersonalTax ' + 'success');
                logger.info(' update driver salaery ' + 'completed');
                
            }
        })
    });
}

module.exports = {
    createDriveSalaryBatch:createDriveSalaryBatch
}