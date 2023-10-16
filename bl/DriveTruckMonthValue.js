/**
 * Created by zwl on 2019/5/29.
 */

var sysMsg = require('../util/SystemMsg.js');
var sysError = require('../util/SystemError.js');
var resUtil = require('../util/ResponseUtil.js');
var driveTruckMonthValueDAO = require('../dao/DriveTruckMonthValueDAO.js');
var truckDAO = require('../dao/TruckDAO.js');
var Seq = require('seq');
var serverLogger = require('../util/ServerLogger.js');
var logger = serverLogger.createLogger('DriveTruckMonthValue.js');
var moment = require('moment/moment.js');



function createDriveTruckMonthValue(req,res,next){
    var params = {} ;
    var myDate = new Date();
    var yMonthDay = new Date(myDate-60*24*60*60*1000);
    var yMonth = moment(yMonthDay).format('YYYYMM');
    Seq().seq(function(){
        var that = this;
        params.yMonth = yMonth;
        driveTruckMonthValueDAO.deleteDriveTruckMonthValue(params,function(error,result){
            if (error) {
                logger.error(' deleteDriveTruckMonthValue ' + error.message);
                throw sysError.InternalError(error.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' deleteDriveTruckMonthValue ' + 'success');
                }else{
                    logger.warn(' deleteDriveTruckMonthValue ' + 'failed');
                }
                that();
            }
        })
    }).seq(function(){
        var that = this;
        params.yMonth = yMonth;
        driveTruckMonthValueDAO.addDistance(params,function(error,result){
            if (error) {
                if(error.message.indexOf("Duplicate") > 0) {
                    resUtil.resetFailedRes(res, "�����Ѿ�����");
                    return next();
                } else{
                    logger.error(' createDriveTruckMonthValue ' + error.message);
                    throw sysError.InternalError(error.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
                }
            } else {
                if(result&&result.insertId>0){
                    logger.info(' createDriveTruckMonthValue ' + 'success');
                }else{
                    logger.warn(' createDriveTruckMonthValue ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        params.yMonth = yMonth;
        driveTruckMonthValueDAO.updateStorageCarCount(params,function(err,result){
            if (err) {
                logger.error(' updateStorageCarCount ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateStorageCarCount ' + 'success');
                }else{
                    logger.warn(' updateStorageCarCount ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        params.yMonth = yMonth;
        driveTruckMonthValueDAO.updateOutput(params,function(err,result){
            if (err) {
                logger.error(' updateOutput ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateOutput ' + 'success');
                }else{
                    logger.warn(' updateOutput ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        params.yMonth = yMonth;
        driveTruckMonthValueDAO.updateInsureFee(params,function(err,result){
            if (err) {
                logger.error(' updateInsureFee ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateInsureFee ' + 'success');
                }else{
                    logger.warn(' updateInsureFee ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        params.yMonth = yMonth;
        driveTruckMonthValueDAO.updateDistanceSalary(params,function(err,result){
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
        params.yMonth = yMonth;
        driveTruckMonthValueDAO.updateDamage(params,function(err,result){
            if (err) {
                logger.error(' updateDamage ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateDamage ' + 'success');
                }else{
                    logger.warn(' updateDamage ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        params.yMonth = yMonth;
        driveTruckMonthValueDAO.updateCleanFee(params,function(err,result){
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
        params.yMonth = yMonth;
        driveTruckMonthValueDAO.updateHotelFee(params,function(err,result){
            if (err) {
                logger.error(' updateHotelFee ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateHotelFee ' + 'success');
                }else{
                    logger.warn(' updateHotelFee ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        params.yMonth = yMonth;
        driveTruckMonthValueDAO.updateEtcFee(params,function(err,result){
            if (err) {
                logger.error(' updateEtcFee ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateHotelFee ' + 'success');
                }else{
                    logger.warn(' updateHotelFee ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        params.yMonth = yMonth;
        driveTruckMonthValueDAO.updateOilFee(params,function(err,result){
            if (err) {
                logger.error(' updateOilFee ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateOilFee ' + 'success');
                }else{
                    logger.warn(' updateOilFee ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        params.yMonth = yMonth;
        driveTruckMonthValueDAO.updatePeccancy(params,function(err,result){
            if (err) {
                logger.error(' updatePeccancy ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updatePeccancy ' + 'success');
                }else{
                    logger.warn(' updatePeccancy ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        params.yMonth = yMonth;
        driveTruckMonthValueDAO.updateRepair2(params,function(err,result){
            if (err) {
                logger.error(' updateRepair2' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateRepair2 ' + 'success');
                }else{
                    logger.warn(' updateRepair2 ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        params.yMonth = yMonth;
        driveTruckMonthValueDAO.updateRepair3(params,function(err,result){
            if (err) {
                logger.error(' updateRepair3' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateRepair3 ' + 'success');
                }else{
                    logger.warn(' updateRepair3 ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        params.yMonth = yMonth;
        driveTruckMonthValueDAO.updateCarOilFee(params,function(err,result){
            if (err) {
                logger.error(' updateCarOilFee ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateCarOilFee ' + 'success');
                }else{
                    logger.warn(' updateCarOilFee ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        var that = this;
        params.yMonth = yMonth;
        driveTruckMonthValueDAO.updateTruckNum(params,function(err,result){
            if (err) {
                logger.error(' updateTruckNum ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                if(result&&result.affectedRows>0){
                    logger.info(' updateCarOilFee ' + 'success');
                }else{
                    logger.warn(' updateCarOilFee ' + 'failed');
                }
                that();
            }
        })
    }).seq(function () {
        params.yMonth = yMonth;
        driveTruckMonthValueDAO.updateDrive(params,function(err,result){
            if (err) {
                logger.error(' updateDrive ' + err.message);
                throw sysError.InternalError(err.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
            } else {
                logger.info(' updateDrive ' + 'success');
            }
        })
    })
}


module.exports = {
    createDriveTruckMonthValue : createDriveTruckMonthValue
}