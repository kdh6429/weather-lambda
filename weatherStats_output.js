'use strict';
// import weatherUtil from './module/weather_util';
// import commonUtil from './module/common_util';

const weatherUtil = require('./module/weather_util');
const commonUtil = require('./module/common_util');
const config = require('./config');
const { ultraSrtFcstBatch } = require('./shortWeather');
const weatherStatsUltra = require('./weatherStats_ultra');
const weatherStatsVilage = require('./weatherStats_vilage');
const weatherStatsMid = require('./weatherStats_mid');

const LOCAL_HISTORY_LENGTH = 30;

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
function delay(interval) {
    return new Promise(resolve => setTimeout(resolve, interval));
}

// 모든 정보가 생성된 이후에 시도해야 함 ( +1 hour : 02 )
module.exports.output = (event, context, callback) => {
    //const {yyyymmdd, hour} = commonUtil.getDateTime(event, -1);
    const statsDocsPromises = [config.map_infos[0]].map( (map_info, index) => new Promise((resolve, reject) => {
        try {
            delay(index * 3000).then(() => {
                console.log( "delay 3000");
                commonUtil.getData("weather-output", "lastest").then(totalDoc => {
                    commonUtil.getData("weather-output", "lastest." + map_info.x + "." + map_info.y).then(localDoc => {
                        get1hourlyDocs(event, map_info, totalDoc, localDoc).then(doc => {
                            console.log( "created doc", doc);
                            //doc["id"] = "1hourly-" + yyyymmdd + "." + hour + "00." + map_info.x + "." + map_info.y;
                            console.log( "doc", doc);
                            importData("weather-hourly-stats", [doc]).then( count => {
                                resolve(count);
                            });
                        })
                    });
                });
            });
        }
        catch(err) {
            reject(err);
        }
    }));
    
    Promise.all(statsDocsPromises).then( result => {
        console.log( "result", result);
        callback(null, response("done" + result));
    }).catch(err=>{
        console.error( "err", err);
        callback(null, response("err" + err));
    });
};

const get1hourlyDocs = async(event, map_info,  totalDoc, localDoc) => new Promise((resolve, reject) => {
    const {yyyymmdd, hour} = commonUtil.getDateTime(event, -1);

    const hourlyStatsKey = "1hourly-" + yyyymmdd + "." + hour + "00." + map_info.x + "." + map_info.y;
    commonUtil.getData("weather-hourly-stats", hourlyStatsKey).then(rtnData => {
        if (commonUtil.isEmpty(rtnData)) {
            console.error("There is no data in weather-hourly-stats. key is ", hourlyStatsKey);
            return ;
        }
        const hourlyStats = commonUtil.removeMissValue(rtnData.Item);

        const timeKeys = ['u1', 'u2', 'u3', 'u4', 'u5', 'u6'];
        for(const timeKey in timeKeys) {
            if( !timeKey in localDoc) {
                localDoc[timeKey] = {
                    'pty' : {'raw':[], 'history' : [],'rate' : 0}, // 날씨
                    'reh' : {'raw':[], 'history' : [],'avg' : 0}, // 습도 
                    'vec' : {'raw':[], 'history' : [],'avg' : 0}, // 풍향
                    't1h' : {'raw':[], 'history' : [],'avg' : 0}, // 온도
                    'rn1' : {'raw':[], 'history' : [],'avg' : 0}, // 강수량
                    'wsd' : {'raw':[], 'history' : [],'avg' : 0}, // 풍량
                    'stmp_w' : {'raw':[], 'history' : [],'avg' : 0}, // 체감온도 겨울
                    'stmp_s' : {'raw':[], 'history' : [],'avg' : 0}, // 체감온도 여름
                }
            }
            if('pty' in hourlyStats[timeKey] && 'pty_d' in hourlyStats[timeKey]) {
                const tmpObj = {
                    't' : commonUtil.convertYMDHtoDate(yyyymmdd, hour),
                    'e' : hourlyStats[timeKey]['pty_d']['e'],
                    'a' : hourlyStats[timeKey]['pty_d']['a'],
                    'f': hourlyStats[timeKey]['pty_d']['e']===hourlyStats[timeKey]['pty_d']['a']
                };
                localDoc[timeKey]['pty']['raw'].push(tmpObj);
                if( localDoc[timeKey]['pty']['raw'].length > LOCAL_HISTORY_LENGTH) {
                    localDoc[timeKey]['pty']['raw'].splice(localDoc[timeKey]['pty']['raw'].length - LOCAL_HISTORY_LENGTH);
                }
            }

            if('reh' in hourlyStats[timeKey] && 'reh_d' in hourlyStats[timeKey]) {
                outputDoc[timeKey]['reh']['history'].push({
                    't' : commonUtil.convertYMDHtoDate(yyyymmdd, hour),
                    'e' : hourlyStats[timeKey]['reh_d']['e'],
                    'a' : hourlyStats[timeKey]['reh_d']['a'],
                    'd': hourlyStats[timeKey]['reh']
                });
                if( localDoc[timeKey]['reh']['history'].length > LOCAL_HISTORY_LENGTH) {
                    localDoc[timeKey]['reh']['history'].splice(localDoc[timeKey]['reh']['history'].length - LOCAL_HISTORY_LENGTH);
                }
            }

            if('vec' in hourlyStats[timeKey] && 'vec_d' in hourlyStats[timeKey]) {
                outputDoc[timeKey]['vec']['history'].push({
                    't' : commonUtil.convertYMDHtoDate(yyyymmdd, hour),
                    'e' : hourlyStats[timeKey]['vec_d']['e'],
                    'a' : hourlyStats[timeKey]['vec_d']['a'],
                    'd': hourlyStats[timeKey]['vec']
                });
                if( localDoc[timeKey]['vec']['history'].length > LOCAL_HISTORY_LENGTH) {
                    localDoc[timeKey]['vec']['history'].splice(localDoc[timeKey]['vec']['history'].length - LOCAL_HISTORY_LENGTH);
                }
            }

            if('t1h' in hourlyStats[timeKey] && 't1h_d' in hourlyStats[timeKey]) {
                outputDoc[timeKey]['t1h']['history'].push({
                    't' : commonUtil.convertYMDHtoDate(yyyymmdd, hour),
                    'e' : hourlyStats[timeKey]['t1h_d']['e'],
                    'a' : hourlyStats[timeKey]['t1h_d']['a'],
                    'd': hourlyStats[timeKey]['t1h']
                });
                if( localDoc[timeKey]['t1h']['history'].length > LOCAL_HISTORY_LENGTH) {
                    localDoc[timeKey]['t1h']['history'].splice(localDoc[timeKey]['t1h']['history'].length - LOCAL_HISTORY_LENGTH);
                }
            }
            if('tn1' in hourlyStats[timeKey] && 'tn1_d' in hourlyStats[timeKey]) {
                if (hourlyStats[timeKey]['tn1_d']['e']!=='1mm 미만' || 
                    hourlyStats[timeKey]['tn1_d']['a']!=='0') {
                    outputDoc[timeKey]['rn1']['history'].push({
                        't' : commonUtil.convertYMDHtoDate(yyyymmdd, hour),
                        'e' : hourlyStats[timeKey]['tn1_d']['e'],
                        'a' : hourlyStats[timeKey]['tn1_d']['a'],
                        'd': hourlyStats[timeKey]['tn1']
                    });
                    if( localDoc[timeKey]['rn1']['history'].length > LOCAL_HISTORY_LENGTH) {
                        localDoc[timeKey]['rn1']['history'].splice(localDoc[timeKey]['rn1']['history'].length - LOCAL_HISTORY_LENGTH);
                    }
                }
            }
            if('wsd' in hourlyStats[timeKey] && 'wsd_d' in hourlyStats[timeKey]) {
                outputDoc[timeKey]['wsd']['history'].push({
                    't' : commonUtil.convertYMDHtoDate(yyyymmdd, hour),
                    'e' : hourlyStats[timeKey]['wsd_d']['e'],
                    'a' : hourlyStats[timeKey]['wsd_d']['a'],
                    'd': hourlyStats[timeKey]['wsd']
                });
                if( localDoc[timeKey]['wsd']['history'].length > LOCAL_HISTORY_LENGTH) {
                    localDoc[timeKey]['wsd']['history'].splice(localDoc[timeKey]['wsd']['history'].length - LOCAL_HISTORY_LENGTH);
                }
            }
            
            function getSensTemp_w(t, v) {
                return 13.12 + 0.6215*t - 11.37 * Math.pow(v, 0.16) + 0.3965 * Math.pow(v, 0.16) * t;
            }
            // 체감 온도 ( 1,2,3,4,5, 10,11,12 월용)
            if( 'wsd' in hourlyStats[timeKey] && 'wsd_d' in hourlyStats[timeKey] &&
                't1h' in hourlyStats[timeKey] && 't1h_d' in hourlyStats[timeKey]) {

                const eTemp = getSensTemp_w(hourlyStats[timeKey]['t1h_d']['e'],hourlyStats[timeKey]['wsd_d']['e']);
                const aTemp = getSensTemp_w(hourlyStats[timeKey]['t1h_d']['a'],hourlyStats[timeKey]['wsd_d']['a']);
                outputDoc[timeKey]['stmp_w']['history'].push({
                    't' : commonUtil.convertYMDHtoDate(yyyymmdd, hour),
                    'e' : eTemp,
                    'a' : aTemp,
                    'd': aTemp - eTemp,
                });

                if( localDoc[timeKey]['stmp_w']['history'].length > LOCAL_HISTORY_LENGTH) {
                    localDoc[timeKey]['stmp_w']['history'].splice(localDoc[timeKey]['stmp_w']['history'].length - LOCAL_HISTORY_LENGTH);
                }
            }

            
            function getSensTemp_s(Ta, Rh) {
                function getTw(Ta, Rh) {
                    return Ta * Math.atan(0.151977 * Math.pow(Rh + 8.313659, 0.5)) + Math.atan(Ta+Rh) - Math.atan(Rh - 1.67633) + (0.00391838 * Math.pow(Rh, 3/2) * Math.atan(0.023101 * Rh)) - 4.686035
                }
                function getSensTemp(Tw, Ta) {
                    return -0.2442 + (0.55399 * Tw) + (0.45535 * Ta) - (0.0022 * Math.pow(Tw, 2)) + (0.00278 * Tw * Ta) + 3.5;
                }
                return getSensTemp( getTw(Ta, Rh), Ta);
            }
            // 체감 온도 (6,7,8,9 월용)
            if( 'reh' in hourlyStats[timeKey] && 'reh_d' in hourlyStats[timeKey] &&
                't1h' in hourlyStats[timeKey] && 't1h_d' in hourlyStats[timeKey]) {

                const eTemp = getSensTemp_s(hourlyStats[timeKey]['t1h_d']['e'],hourlyStats[timeKey]['reh_d']['e']);
                const aTemp = getSensTemp_s(hourlyStats[timeKey]['t1h_d']['a'],hourlyStats[timeKey]['reh_d']['a']);
                outputDoc[timeKey]['stmp_s']['history'].push({
                    't' : commonUtil.convertYMDHtoDate(yyyymmdd, hour),
                    'e' : eTemp,
                    'a' : aTemp,
                    'd': aTemp - eTemp,
                });

                if( localDoc[timeKey]['stmp_s']['history'].length > LOCAL_HISTORY_LENGTH) {
                    localDoc[timeKey]['stmp_s']['history'].splice(localDoc[timeKey]['stmp_s']['history'].length - LOCAL_HISTORY_LENGTH);
                }
            }



        }
        resolve(outputDoc);
    });
});


const importData = (tableName, docs) => new Promise((resolve, reject) => {
    commonUtil.importData( tableName, docs).then( count => {
        //console.log("upload ", count, " docs");
        resolve(count);
        //callback(null, response("upload " + count + " docs"));
    }).catch(err => {
        console.warn("Unable to insert " + tableName + " data.", err);
        reject("Unable to insert " + tableName + " data." + err);
        //callback(null, response("Unable to insert " + tableName + " data." + err));
    });;
});
