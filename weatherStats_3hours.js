
// 'use strict';
// const commonUtil = require('./module/common_util');

// const getMidStatsHistoryTimes = (event) => {
//     var curToday = new Date();
//     var hour = 99;
//     if(event.queryStringParameters && !isNaN(event.queryStringParameters.hour)) {
//         hour = +event.queryStringParameters.hour;
//         //console.log("[SET] Custom hour '" + hour + "' set.");
//         curToday.setHours(hour);
//     }
//     else {
//         curToday.setHours(curToday.getHours() + 9);
//     }

//     const tmpDay = new Date(curToday);
//     if ( tmpDay.getHours() >= 0 && tmpDay.getHours() <= 11) tmpDay.setHours( 6);
//     if ( tmpDay.getHours() >= 12 && tmpDay.getHours() <= 23) tmpDay.setHours( 18);
    
//     tmpDay.setTime(tmpDay.getTime() - (24 * 3 * 60 * 60 * 1000));
//     var dateTimes = [];
//     var fromTmpDay = new Date( tmpDay.getTime() - (10*24*60*60*1000));
//     while(tmpDay >= fromTmpDay) {
//         const {yyyymmdd, hour} = commonUtil.getYMDandHour(tmpDay);
//         dateTimes.push( {yyyymmdd, hour : "06"});
//         dateTimes.push( {yyyymmdd, hour : "18"});
//         tmpDay.setTime(tmpDay.getTime() - (24 * 60 * 60 * 1000));
//     };
//     return dateTimes;
// }

// module.exports.getTaDocs  = async(event, curWeather, map_info) => new Promise((resolve, reject) => {
//     const {yyyymmdd, hour} = commonUtil.getDateTime(event);
//     const dateTimes = getMidStatsHistoryTimes(event);
    
//     const getDocsPromises = dateTimes.map(dateTime => {
//         const key = dateTime.yyyymmdd + "." + dateTime.hour + "00." + map_info.midTa;
//         return commonUtil.getData("midTa", key);
//     });

//     let resultDoc = {};
//     Promise.all(getDocsPromises).then(docs => {
//         docs.forEach( doc => {
//             if (commonUtil.isEmpty(doc)) return;
//             const fWeather = commonUtil.removeMissValue(doc.Item);
//             const diff = commonUtil.getDayDiff(fWeather.id.substring(0, 8), yyyymmdd);
//             const objectKey = "ta" + diff;
//             resultDoc[objectKey] = {}
            
//             const maxKey = "taMax" + diff;
//             const minKey = "taMin" + diff;
            
//             if (fWeather[maxKey] && fWeather[minKey] && curWeather.T1H
//                 && fWeather[maxKey] < 900 && fWeather[maxKey] > -900 
//                 && fWeather[minKey] < 900 && fWeather[minKey] > -900 
//                 && curWeather.T1H < 900 && curWeather.T1H > -900
//             ) {
//                 const t1h = (() => {
//                     if (fWeather[maxKey] < curWeather.T1H){
//                         return ((+curWeather.T1H) - (+fWeather[maxKey])).toFixed(1);
//                     }
//                     else if (fWeather[minKey] > curWeather.T1H) {
//                         return ((+curWeather.T1H) - (+fWeather[minKey])).toFixed(1);
//                     }
//                     return 0;
//                 })();
//                 Object.assign( resultDoc[objectKey], {
//                     ["ta"] : t1h,
//                     ["ta" + diff + "_d"] : {
//                         "min" : fWeather[minKey],
//                         "max" : fWeather[maxKey],
//                         "a" : curWeather.PTY,
//                     }
//                 });
//             }
//         });
//         resolve(resultDoc);
//     });
// });


// module.exports.getLandFDocs  = async(event, curWeather, map_info) => new Promise((resolve, reject) => {
//     const {yyyymmdd, hour} = commonUtil.getDateTime(event);
//     const dateTimes = getMidStatsHistoryTimes(event);
    
//     const getDocsPromises = dateTimes.map(dateTime => {
//         const key = dateTime.yyyymmdd + "." + dateTime.hour + "00." + map_info.midLand;
//         return commonUtil.getData("midLandFcst", key);
//     });

//     let resultDoc = {};
//     Promise.all(getDocsPromises).then(docs => {
//         docs.forEach( doc => {
//             if (commonUtil.isEmpty(doc)) return;
//             const fWeather = commonUtil.removeMissValue(doc.Item);
//             const diff = commonUtil.getDayDiff(fWeather.id.substring(0, 8), yyyymmdd);
//             const objectKey = "ta" + diff;
//             resultDoc[objectKey] = {}
            
//             const mainKey = (() => {
//                 if (diff <= 7) {
//                     if(hour <= 11) { //0~11
//                         return diff + "Am";
//                     }
//                     return diff + "Pm";
//                 }
//                 else if (diff > 7) {
//                     return diff;
//                 }
//             })();

//             if (fWeather["rnSt" + mainKey] && curWeather.RN1) {
//                 const rnSt = (() => {
//                     if (fWeather["rnSt" + mainKey] >= 50) {
//                         if ((+curWeather.RN1) > 0) return "T";
//                         return "F";
//                     }
//                     else if (fWeather["rnSt" + mainKey] < 50) {
//                         if ((+curWeather.RN1) > 0) return "F";
//                         return "T";
//                     }
//                     return "N";
//                 })();
//                 Object.assign( resultDoc[objectKey], {
//                     ["rnSt"] : rnSt,
//                     ["rnSt" + diff + "_d"] : {
//                         "e" : fWeather["rnSt" + mainKey],
//                         "a" : curWeather.RN1,
//                     }
//                 });
//             }

//             if (fWeather["wf" + mainKey] && curWeather.RN1) {
//                 const wf = (() => {
//                     if (fWeather["wf" + mainKey].includes("비")) {
//                         if ((+curWeather.RN1) > 0) return "T";
//                         return "F";
//                     }
//                     else {
//                         if ((+curWeather.RN1) > 0) return "F";
//                         return "T";
//                     }
//                 })();
//                 Object.assign( resultDoc[objectKey], {
//                     ["wf"] : wf,
//                     ["wf" + diff + "_d"] : {
//                         "e" : fWeather["wf" + mainKey],
//                         "a" : curWeather.RN1,
//                     }
//                 });
//             }
//         });
//         resolve(resultDoc);
//     });
// });