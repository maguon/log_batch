/**
 * Created by lingxue on 2017/4/11.
 */
const later = require('later');
const Seq = require('seq');
const sysConfig = require('./config/SystemConfig.js');
const serverLogger = require('./util/ServerLogger.js');
const dateBl = require('./bl/DateBl.js');
const dispatchBl = require('./bl/DispatchBl.js');
const logger = serverLogger.createLogger('Batch.js');


later.date.localTime();
logger.info('Log batch start at '+ (new Date()).toLocaleString());
const startBasic = {h:[0],m: [0],s:[5]};
//const startBasic = {s:[29,59]};
const startComposite = [startBasic];
const startSched =  {
    schedules:startComposite
};

const endBasic ={h:[16],m: [14],s:[5]};
const endComposite = [endBasic];
const endSched = {
    schedules:endComposite
}


try{
    later.setInterval(()=>{

        dateBl.saveStatDate((err,result) =>{
            if(err){
                logger.error('create new date error:'+err.stack);
            }else{
                if(result && result.success){
                    logger.info('create new date success');
                }else{
                    logger.info('create new date false');
                }
            }
        });
        dateBl.initStorageStat((err,result)=>{
            if(err){
                logger.error('create new storage balance error:'+err.stack);
            }else{
                if(result && result.affectedRows){
                    logger.info('create new storage balance success');
                }else{
                    logger.info('create new storage balance false');
                }
            }
        })
        dispatchBl.completeTaskStat((err,result) =>{
            if(err){
                logger.error('completeTaskStat error:'+err.stack);
            }else{
                if(result && result.affectedRows){
                    logger.info('completeTaskStat success');
                }else{
                    logger.info('completeTaskStat false');
                }
            }
        });
        dispatchBl.completeTaskStatByDate((err,result) =>{
            if(err){
                logger.error('completeTaskStat error:'+err.stack);
            }else{
                if(result && result.affectedRows){
                    logger.info('completeTaskStat success');
                }else{
                    logger.info('completeTaskStat false');
                }
            }
        })
        dispatchBl.completeDemandInfoByDate((err,result) =>{
            if(err){
                logger.error('completeTaskStat error:'+err.stack);
            }else{
                if(result && result.affectedRows){
                    logger.info('completeDemand success');
                }else{
                    logger.info('completeDemand false');
                }
            }
        })
    },startSched);
}catch(err){
    logger.error(err.stack);
}