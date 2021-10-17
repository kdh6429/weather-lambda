'use strict';
// import weatherUtil from './module/weather_util';
// import commonUtil from './module/common_util';
// commit test2
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

    Promise.allSettled(getDataPromises).then( datas=> {
        const docs = datas.filter( d=> d.status === 'fulfilled').map( d=> d.value).flat();
        imrpotData("ultraSrtFcst", docs, callback);
    });
};

module.exports.ultraSrtNcstBatch = (event, context, callback) => {
    const {yyyymmdd, hour} = commonUtil.getDateTime(event);
    const getDataPromises = config.map_infos.map(map_info => weatherUtil.getUltraSrtNcst(yyyymmdd, hour+"00", map_info.x, map_info.y));

    Promise.allSettled(getDataPromises).then( datas=> {
        const docs = datas.filter( d=> d.status === 'fulfilled').map( d=> d.value).flat();
        imrpotData("ultraSrtNcst", docs, callback);
    });
};


module.exports.vilageFcstBatch = (event, context, callback) => {
    const {yyyymmdd, hour} = commonUtil.getDateTime(event);
    const getDataPromises = config.map_infos.map(map_info => weatherUtil.getVilageFcst(yyyymmdd, hour+"00", map_info.x, map_info.y));
    
    Promise.allSettled(getDataPromises).then( datas=> {
        const docs = datas.filter( d=> d.status === 'fulfilled').map( d=> d.value).flat();
        imrpotData("vilageFcst", docs, callback);
    });
};

module.exports.midTaBatch = (event, context, callback) => {
    const {yyyymmdd, hour} = commonUtil.getDateTime(event);
    const getDataPromises = config.map_infos.map(map_info => weatherUtil.getMidTa(yyyymmdd, hour+"00", map_info.midTa));

    Promise.allSettled(getDataPromises).then( datas=> {
        const docs = datas.filter( d=> d.status === 'fulfilled').map( d=> d.value).flat();
        imrpotData("midTa", docs, callback);
    });
};

module.exports.midLandFcstBatch = (event, context, callback) => {
    const {yyyymmdd, hour} = commonUtil.getDateTime(event);
    const getDataPromises = config.map_infos.map(map_info => weatherUtil.getMidLandFcst(yyyymmdd, hour+"00", map_info.midLand));

    Promise.allSettled(getDataPromises).then( datas=> {
        const docs = datas.filter( d=> d.status === 'fulfilled').map( d=> d.value).flat();
        imrpotData("midLandFcst", docs, callback);
    });
};

const imrpotData = (tableName, docs, callback) => {
    commonUtil.importData( tableName, docs).then( count => {
        console.log("upload ", count, " docs");

        commonUtil.sendtoSlack(tableName + ":" + count).then( () => {
            callback(null, response(tableName + ":" + count));
        });
    });
}