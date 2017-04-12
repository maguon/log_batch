'use strict'
const moment = require('moment');
const getDateFormat = (date ,format) => {
    var o = {
        "M+" : date.getMonth()+1,
        "d+" : date.getDate(),
        "h+" : date.getHours(),
        "m+" : date.getMinutes(),
        "s+" : date.getSeconds(),
        "q+" : Math.floor((date.getMonth()+3)/3),
        "S"  : date.getMilliseconds()
    };
    if(/(y+)/.test(format))
        format=format.replace(RegExp.$1, (date.getFullYear()+"").substr(4 - RegExp.$1.length));
    for(var k in o)
        if(new RegExp("("+ k +")").test(format))
            format = format.replace(RegExp.$1, (RegExp.$1.length==1) ? (o[k]) : (("00"+ o[k]).substr((""+ o[k]).length)));
    return format;
}
const  padLeft = (str,lenght)=>{
    if(str.length >= lenght)
        return str;
    else
        return padLeft("0" +str,lenght);
}

const getLastDayLong = ()=>{
    let currentDate = new Date();
    let currentDateStr = moment(currentDate).format("YYYY-MM-DD");
    let currentDayLong = new Date(currentDateStr).getTime();
    let dayLong  = 23*60*60*1000;
    let yesterdayDayLong = currentDayLong -dayLong;
    return yesterdayDayLong;
}


function getWeekByDate(){
    let d1 = new Date();
    let d2 = new Date();
    d2.setMonth(0);
    d2.setDate(1);
    d2.setDate(7-d2.getDay());
    let rq = d1-d2;
    let s1 = Math.ceil(rq/(24*60*60*1000));
    let s2 = Math.ceil(s1/7);
    return s2+1;
}
module.exports = { getDateFormat ,padLeft ,getLastDayLong ,getWeekByDate }