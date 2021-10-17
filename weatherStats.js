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

// 해당시간이 거의다 끝난 시점에 호출해야 함 50
module.exports.gen1HourStats = (event, context, callback) => {
    const {yyyymmdd, hour} = commonUtil.getDateTime(event);
    const statsDocsPromises = config.map_infos.map( (map_info, index) => new Promise((resolve, reject) => {
        try {
            delay(index * 2000).then(() => {
                weatherStatsUltra.getDocs(event, map_info).then(doc => {
                    doc["id"] = "1hourly-" + yyyymmdd + "." + hour + "00." + map_info.x + "." + map_info.y;
                    importData("weather-hourly-stats", [doc]).then( count => {
                        resolve(count);
                    });
                })
            });
        }
        catch(err) {
            reject(err);
        }
    }));
    
    Promise.all(statsDocsPromises).then( result => {
        const resultCount = result.reduce((a, b)=> a+b, 0);
        
        commonUtil.sendtoSlack("weather-hourly-stats gen1HourStats : " + resultCount).then( () => {
            callback(null, response("weather-hourly-stats gen1HourStats : " + resultCount));
        });
    }).catch(err=>{
        console.error( "err", err);
        callback(null, response("err" + err));
    });
};

module.exports.gen3HourStats = (event, context, callback) => {
    const {yyyymmdd, hour} = commonUtil.getDateTime(event, -1);
    const statsDocsPromises = config.map_infos.map( (map_info, index) => new Promise((resolve, reject) => {
        try {
            delay(index * 2000).then(() => {
                weatherStatsVilage.getDocs(event, map_info).then(doc => {
                    doc["id"] = "3hourly-" + yyyymmdd + "." + hour + "00." + map_info.x + "." + map_info.y;
                    console.log( "doc", doc);
                    importData("weather-hourly-stats", [doc]).then( count => {
                        resolve(count);
                    });
                })
            });
        }
        catch(err) {
            reject(err);
        }
    }));
    
    Promise.all(statsDocsPromises).then( result => {
        const resultCount = result.reduce((a, b)=> a+b, 0);
        
        commonUtil.sendtoSlack("weather-hourly-stats gen3HourStats : " + resultCount).then( () => {
            callback(null, response("weather-hourly-stats gen3HourStats : " + resultCount));
        });

    }).catch(err=>{
        console.error( "err", err);
        callback(null, response("err" + err));
    });
};


module.exports.gen24HourStats = (event, context, callback) => {
    const {yyyymmdd, hour} = commonUtil.getDateTime(event, -1);
    const statsDocsPromises = config.map_infos.map( (map_info, index) => new Promise((resolve, reject) => {
        try {
            delay(index * 2000).then(() => {
                weatherStatsMid.getLandFDocs(event, map_info).then(doc => {
                    doc["id"] = "24hourly-" + yyyymmdd + "." + hour + "00." + map_info.x + "." + map_info.y;
                    console.log( "doc", doc);
                    importData("weather-hourly-stats", [doc]).then( count => {
                        resolve(count);
                    });
                })
            });
        }
        catch(err) {
            reject(err);
        }
    }));
    
    Promise.all(statsDocsPromises).then( result => {
        const resultCount = result.reduce((a, b)=> a+b, 0);
        
        commonUtil.sendtoSlack("weather-hourly-stats gen24HourStats : " + resultCount).then( () => {
            callback(null, response("weather-hourly-stats gen24HourStats : " + resultCount));
        });

    }).catch(err=>{
        console.error( "err", err);
        callback(null, response("err" + err));
    });
};

/*

//.catch(console.error);


// weatherStatsMid.getLandFDocs(event, map_info).then(tDoc => {



// weatherStatsVilage.getDocs(event, curWeather, map_info).then(vDoc => {
//     console.log( "created vDoc", vDoc);
//     weatherStatsMid.getTaDocs(event, curWeather, map_info).then(tDoc => {
//         console.log( "created tDoc", tDoc);
//         weatherStatsMid.getLandFDocs(event, curWeather, map_info).then(landFDoc => {
//             console.log( "created landFDoc", landFDoc);
//             const rtnDoc = Object.assign({}, uDoc, vDoc, tDoc, landFDoc);
//             rtnDoc["id"] = yyyymmdd + "." + hour + "00." + map_info.x + "." + map_info.y;
//             console.log( "id", rtnDoc["id"]);
//             //console.log( "rtnDoc", rtnDoc);
//             importData("weather-hourly-stats", [rtnDoc]).then( count => {
//                 resolve(count);
//             });
//         })//.catch(console.error);
//     })//.catch(console.error);
// })//.catch(console.error);
*/



// module.exports.dailyStats = (event, context, callback) => {
//     const {yyyymmdd, hour} = commonUtil.getDateTime(event);
//     const YMDHours = commonUtil.getBefore3DateTime(event);

//     // u1 u3 u6 
//     // v11 v12 v13
//     // v23 v24 v25
//     // v35 v36 v37

//     // yyyymmdd + "." + hour + "00." + map_info.x + "." + map_info.y;

//     let resultDoc = [];
//     const statsDocsPromises = config.map_infos.map( (map_info, index) => new Promise((resolve, reject) => {
//         try {
//             const id = "3hourStats-" + yyyymmdd + "." + hour + "00." + map_info.x + "." + map_info.y;

//             const curWeatherKey = yyyymmdd + "." + hour + "00." + map_info.x + "." + map_info.y;
//             commonUtil.getData("weather-hourly-stats", curWeatherKey).then(rtnData => {
//                 if (commonUtil.isEmpty(rtnData)) {
//                     console.error("There is no current weather data in ultraSrtNcst. key is ", curWeatherKey);
//                     return ;
//                 }
//             });
//         }
//         catch(err) {
//             reject(err);
//         }
//     }));
// }



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
