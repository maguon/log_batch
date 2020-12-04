const totalMonthStat = require('./bl/TotalMonthStat.js');

const yMonth =process.argv[3];
totalMonthStat.createDriveTruckMonthValue(yMonth);