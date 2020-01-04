const Seq = require('seq');
const fs = require('fs');
const sysMsg = require('./util/SystemMsg.js');
const sysError = require('./util/SystemError.js');
const serverLogger = require('./util/ServerLogger.js');
const logger = serverLogger.createLogger('GetUpDistanceFile.js');
const dispatchDAO = require('./dao/DispatchDAO');


let csvString = ''
Seq().seq(function () {
    var that =this;
    dispatchDAO.getUpDistanceTask({},function(error,rows){
        if (error) {
            logger.error(' deleteDriveTruckMonthValue ' + error.message);
            throw sysError.InternalError(error.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
        } else {
            Seq(rows).seqEach(function(rowObj,i){
                var that = this;
                csvString += rowObj.id+',';
                dispatchDAO.getRouteRecord({id:rowObj.id},function(error,rows){
                    if (error) {
                        logger.error(' deleteDriveTruckMonthValue ' + error.message);
                        throw sysError.InternalError(error.message,sysMsg.SYS_INTERNAL_ERROR_MSG);
                    } else {
                        const strArray = rows[0].comment;
                        let strTmp = '';
                        strArray.forEach((obj,i)=>{
                            strTmp += moment(obj.timez).format('YYYY-MM-DD HH:mm:ss'); + ' ' +obj.name +' '+obj.content +' |';
                        })
                        csvString += strTmp +'\r\n';
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
})