'use strict';
// import weatherUtil from './module/weather_util';
// import commonUtil from './module/common_util';
// commit test
const weatherUtil = require('./module/weather_util');
const commonUtil = require('./module/common_util');
const config = require('./config');

const response = (message) => {
    return {
        statusCode: 200,
        headers: {
            'Access-Control-Allow-Origin': '*', // Required for CORS support to work
        },
        body: JSON.stringify({
            message: message,
        }),
    };
}

module.exports.ultraSrtFcstBatch = (event, context, callback) => {
    const {yyyymmdd, hour} = commonUtil.getDateTime(event);
    const getDataPromises = config.map_infos.map(map_info => weatherUtil.getUltraSrtFcst(yyyymmdd, hour+"30", map_info.x, map_info.y));

    Promise.all(getDataPromises).then( datas=> {
        const docs = datas.flat();
        imrpotData("ultraSrtFcst", docs, callback);
    });
};

module.exports.ultraSrtNcstBatch = (event, context, callback) => {
    const {yyyymmdd, hour} = commonUtil.getDateTime(event);
    const getDataPromises = config.map_infos.map(map_info => weatherUtil.getUltraSrtNcst(yyyymmdd, hour+"00", map_info.x, map_info.y));

    Promise.all(getDataPromises).then( datas=> {
        const docs = datas.flat();
        imrpotData("ultraSrtNcst", docs, callback);
    });
};


module.exports.vilageFcstBatch = (event, context, callback) => {
    const {yyyymmdd, hour} = commonUtil.getDateTime(event);
    const getDataPromises = config.map_infos.map(map_info => weatherUtil.getVilageFcst(yyyymmdd, hour+"00", map_info.x, map_info.y));
    
    Promise.all(getDataPromises).then( datas=> {
        const docs = datas.flat();
        imrpotData("vilageFcst", docs, callback);
    });
};

module.exports.midTaBatch = (event, context, callback) => {
    const {yyyymmdd, hour} = commonUtil.getDateTime(event);
    const getDataPromises = config.map_infos.map(map_info => weatherUtil.getMidTa(yyyymmdd, hour+"00", map_info.midTa));

    Promise.all(getDataPromises).then( datas=> {
        const docs = datas.flat();
        imrpotData("midTa", docs, callback);
    });
};

module.exports.midLandFcstBatch = (event, context, callback) => {
    const {yyyymmdd, hour} = commonUtil.getDateTime(event);
    const getDataPromises = config.map_infos.map(map_info => weatherUtil.getMidLandFcst(yyyymmdd, hour+"00", map_info.midLand));

    Promise.all(getDataPromises).then( datas=> {
        const docs = datas.flat();
        imrpotData("midLandFcst", docs, callback);
    });
};

// const getDateTime = (event) => {
//     var today = new Date();
//     var hour = 99;

//     if(event.queryStringParameters && !isNaN(event.queryStringParameters.hour)) {
//         hour = +event.queryStringParameters.hour;
//     }
//     else {
//         today.setHours(today.getHours() + 9);
//         hour = today.getHours();
//     }

//     var dd = today.getDate();
//     var mm = today.getMonth()+1;
//     var yyyy = today.getFullYear();
//     var minutes = today.getMinutes();
  
//     if(hour<10) {
//         hour='0'+hour;
//     }
//     if(mm<10) {
//         mm='0'+mm;
//     }
//     if(dd<10) {
//         dd='0'+dd;
//     } 
//     const yyyymmdd = yyyy + "" + mm + "" + dd;
//     return {yyyymmdd, hour}
// }

const imrpotData = (tableName, docs, callback) => {
    commonUtil.importData( tableName, docs).then( count => {
        console.log("upload ", count, " docs");
        callback(null, response("upload " + count + " docs"));
    });
}