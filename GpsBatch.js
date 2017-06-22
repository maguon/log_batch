'use strict'
const gps = require('./bl/Gps.js');
const later = require('later');
const serverLogger = require('./util/ServerLogger.js');
const logger = serverLogger.createLogger('GpsBatch.js');

var quarterBasic = {m: [1,16,31,46]};
const quarterComposite = [quarterBasic];
const quarterSched =  {
    schedules:quarterComposite
};



later.setInterval(()=>{
    gps.saveBizTruck((err,result)=>{
        if(err){
            logger.error('saveBizTruck' + err.stack)
        }else{
            logger.info('saveBizTruck' + 'success ');
        }

    })
},quarterSched);