/**
 * Created by yym on 2020/12/4.
 */

var sysMsg = require('../util/SystemMsg.js');
var sysError = require('../util/SystemError.js');
var resUtil = require('../util/ResponseUtil.js');
var totalMonthStatDAO = require('../dao/TotalMonthStatDAO.js');
var Seq = require('seq');
var moment = require('moment/moment.js');
var serverLogger = require('../util/ServerLogger.js');
var logger = serverLogger.createLogger('TotalMonthStatDAO.js');


function createDriveTruckMonthValue(yMonth){
    var params = {yMonth:yMonth} ;
    Seq().seq(function(){
        var that = this;
        totalMonthStatDAO.deleteTotalMonthStat(params,function(error,result){
            if (error) {
                logger.error(' deleteTotalMonthStat ' + error.message);
                throw sysError.InternalError(error.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' deleteTotalMonthStat ' + 'success');
                }else{
                    logger.warn(' deleteTotalMonthStat ' + 'failed');
                }
                that();
            }
        })
    }).seq(function(){
        var that = this;
        totalMonthStatDAO.addTotalMonthStat(params,function(error,result){
            if (error) {
                if(error.message.indexOf("Duplicate") > 0) {
                    resUtil.resetFailedRes(res, "数据已经存在");
                    return next();
                } else{
                    logger.error(' addTotalMonthStat ' + error.message);
                    throw sysError.InternalError(error.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
                }
            } else {
                if(result&&result.insertId>0){
                    logger.info(' addTotalMonthStat ' + 'success');
                }else{
                    logger.warn(' addTotalMonthStat ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        //商品车数量
        totalMonthStatDAO.updateCarCount(params,function(err,result){
            if (err) {
                logger.error(' updateCarCount ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateCarCount ' + 'success');
                }else{
                    logger.warn(' updateCarCount ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        //产值
        totalMonthStatDAO.updateOutputCount(params,function(err,result){
            if (err) {
                logger.error(' updateOutputCount ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateOutputCount ' + 'success');
                }else{
                    logger.warn(' updateOutputCount ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        //运营货车数量 , 重载公里数 , 空载公里数 , 总公里数 , 重载率
        totalMonthStatDAO.updateTruckCount(params,function(err,result){
            if (err) {
                logger.error(' updateTruckCount ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateTruckCount ' + 'success');
                }else{
                    logger.warn(' updateTruckCount ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        /*外协商品车数量1 , 外协费用1 结算直接查询*/
        totalMonthStatDAO.updateOuterCarCount(params,function(err,result){
            if (err) {
                logger.error(' updateOuterCarCount ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateOuterCarCount ' + 'success');
                }else{
                    logger.warn(' updateOuterCarCount ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        /*外协商品车数量2 , 外协费用2 路线查询费用*/
        totalMonthStatDAO.updateOuterRouteCarCount(params,function(err,result){
            if (err) {
                logger.error(' updateOuterRouteCarCount ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateOuterRouteCarCount ' + 'success');
                }else{
                    logger.warn(' updateOuterRouteCarCount ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        /*外协产值1 结算直接查询*/
        totalMonthStatDAO.updateOuterOutput(params,function(err,result){
            if (err) {
                logger.error(' updateOuterOutput ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateOuterOutput ' + 'success');
                }else{
                    logger.warn(' updateOuterOutput ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        /*外协产值2 路线查询费用*/
        totalMonthStatDAO.updateOuterRouteOutput(params,function(err,result){
            if (err) {
                logger.error(' updateOuterRouteOutput ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateOuterRouteOutput ' + 'success');
                }else{
                    logger.warn(' updateOuterRouteOutput ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        //过路费
        totalMonthStatDAO.updateEtcFeeCount(params,function(err,result){
            if (err) {
                logger.error(' updateEtcFeeCount ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateEtcFeeCount ' + 'success');
                }else{
                    logger.warn(' updateEtcFeeCount ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        //加油量 , 加油费 , 尿素量 , 尿素费
        totalMonthStatDAO.updateOilCount(params,function(err,result){
            if (err) {
                logger.error(' updateOilCount ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateOilCount ' + 'success');
                }else{
                    logger.warn(' updateOilCount ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        //修车费 , 零件费 , 保养费
        //内部维修数 , 内部维修费 , 在外维修次数 , 在外维修数
        totalMonthStatDAO.updateRepairCount(params,function(err,result){
            if (err) {
                logger.error(' updateRepairCount ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateRepairCount ' + 'success');
                }else{
                    logger.warn(' updateRepairCount ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        //处罚次数 , 处罚分数 , 买分金额 , 交通罚款 ,
        //处理金额 , 司机承担罚款 , 公司承担
        totalMonthStatDAO.updatePeccancyCount(params,function(err,result){
            if (err) {
                logger.error(' updatePeccancyCount ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updatePeccancyCount ' + 'success');
                }else{
                    logger.warn(' updatePeccancyCount ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        //质损数 , 个人承担质损费 , 公司承担质损费 , 质损总成本
        totalMonthStatDAO.updateDamageCount(params,function(err,result){
            if (err) {
                logger.error(' updateDamageCount ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateDamageCount ' + 'success');
                }else{
                    logger.warn(' updateDamageCount ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        //洗车费
        params.startDate = params.yMonth.substr(0,4) + '-' + params.yMonth.substr(4,2) + '-01';
        params.lastDateTime = moment(params.yMonth+'01').endOf('month').format("YYYY-MM-DD") ;
        totalMonthStatDAO.updateCleanFeeCount(params,function(err,result){
            if (err) {
                logger.error(' updateCleanFeeCount ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateCleanFeeCount ' + 'success');
                }else{
                    logger.warn(' updateCleanFeeCount ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        //进门费 , 拖车费 , 商品车停车费 , 地跑费 , 带路费
        totalMonthStatDAO.updateDriveTruckFeeCount(params,function(err,result){
            if (err) {
                logger.error(' updateDriveTruckFeeCount ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateDriveTruckFeeCount ' + 'success');
                }else{
                    logger.warn(' updateDriveTruckFeeCount ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        //单车产值 , 单公里产值
        totalMonthStatDAO.updatePerOutputCount(params,function(err,result){
            if (err) {
                logger.error(' updatePerOutputCount ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updatePerOutputCount ' + 'success');
                }else{
                    logger.warn(' updatePerOutputCount ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        //单车质损成本 , 单车公司承担成本
        totalMonthStatDAO.updatePerCarDamageMoneyCount(params,function(err,result){
            if (err) {
                logger.error(' updatePerCarDamageMoneyCount ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updatePerCarDamageMoneyCount ' + 'success');
                }else{
                    logger.warn(' updatePerCarDamageMoneyCount ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        //质损率
        totalMonthStatDAO.updateDamageRatioCount(params,function(err,result){
            if (err) {
                logger.error(' updateDamageRatioCount ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateDamageRatioCount ' + 'success');
                }else{
                    logger.warn(' updateDamageRatioCount ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        //单车洗车费
        totalMonthStatDAO.updatePerCarCleanFeeCount(params,function(err,result){
            if (err) {
                logger.error(' updatePerCarCleanFeeCount ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                logger.info(' updatePerCarCleanFeeCount ' + 'success');
            }
        })
    })
}

module.exports = {
    createDriveTruckMonthValue : createDriveTruckMonthValue
}