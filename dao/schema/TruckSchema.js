'use strict'

const mongoose = require('../../db/connection/MongoCon.js').getMongo();
const Schema = mongoose.Schema;
const ObjectId = Schema.ObjectId;


const truckEntity = new Schema({
    vhe_no : String,
    sim : String ,
    lat: Number,
    lon : Number,
    vel : Number,
    angle : Number,
    oil : Number,
    mileage : Number,
    updated_on : Date
});

module.exports = {truckEntity}