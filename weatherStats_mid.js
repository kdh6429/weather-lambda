
'use strict';
const commonUtil = require('./module/common_util');

const getMidStatsHistoryTimes = (event) => {
    const {yyyymmdd, hour} = commonUtil.getDateTime(event);
    var curToday = commonUtil.convertYMDHtoDate(yyyymmdd, hour);
    
    const tmpDay = new Date(curToday);
    //if ( tmpDay.getHours() >= 0 && tmpDay.getHours() <= 11) tmpDay.setHours( 6);
    //if ( tmpDay.getHours() >= 12 && tmpDay.getHours() <= 23) tmpDay.setHours( 18);
    tmpDay.setHours( 18);

    tmpDay.setTime(tmpDay.getTime() - (24 * 3 * 60 * 60 * 1000));
    var dateTimes = [];
    var fromTmpDay = new Date( tmpDay.getTime() - (10*24*60*60*1000));
    while(tmpDay >= fromTmpDay) {
        const {yyyymmdd, hour} = commonUtil.getYMDandHour(tmpDay);
        //dateTimes.push( {yyyymmdd, hour : "06"});
        dateTimes.push( {yyyymmdd, hour : "18"});
        tmpDay.setTime(tmpDay.getTime() - (24 * 60 * 60 * 1000));
    };
    return dateTimes;
}

module.exports.getTaDocs  = async(event, map_info) => new Promise((resolve, reject) => {
    const {yyyymmdd, hour} = commonUtil.getDateTime(event);
    const dateTimes = getMidStatsHistoryTimes(event);

    const curWeatherKeys = [
        commonUtil.getYMDandHour( commonUtil.getDate(event, -1)),
        commonUtil.getYMDandHour( commonUtil.getDate(event, -2)),
        commonUtil.getYMDandHour( commonUtil.getDate(event, -3)),
        commonUtil.getYMDandHour( commonUtil.getDate(event, -4)),
        commonUtil.getYMDandHour( commonUtil.getDate(event, -5)),
        commonUtil.getYMDandHour( commonUtil.getDate(event, -6)),
        commonUtil.getYMDandHour( commonUtil.getDate(event, -7)),
        commonUtil.getYMDandHour( commonUtil.getDate(event, -8)),
        commonUtil.getYMDandHour( commonUtil.getDate(event, -9)),
        commonUtil.getYMDandHour( commonUtil.getDate(event, -10)),
        commonUtil.getYMDandHour( commonUtil.getDate(event, -11)),
        commonUtil.getYMDandHour( commonUtil.getDate(event, -12)),
    ];
    const curWeatherPromises = curWeatherKeys.map( tmpYMDH => {
        return commonUtil.getData("ultraSrtNcst", tmpYMDH.yyyymmdd + "." + tmpYMDH.hour + "00." + map_info.x + "." + map_info.y);
    });


    Promise.all(getDocsPromises).then(rtnDatas => {
        if (rtnDatas.length === 0) {
            console.error("There is no current weather data in ultraSrtNcst. key is ", curWeatherKey);
            return ;
        }
        const curWeathers = rtnDatas.map(rtnData => {
            return commonUtil.removeMissValue(rtnData.Item)
        }).filter(c=>c);

        const getDocsPromises = dateTimes.map(dateTime => {
            const key = dateTime.yyyymmdd + "." + dateTime.hour + "00." + map_info.midTa;
            return commonUtil.getData("midTa", key);
        });

        let resultDoc = {};
        Promise.all(getDocsPromises).then(docs => {
            // if (commonUtil.isEmpty(doc)) return;
            // const fWeather = commonUtil.removeMissValue(doc.Item);
            // const diff = commonUtil.getHourDiff(fWeather.baseDate, fWeather.baseTime.substring(0,2), fWeather.fcstDate, fWeather.fcstTime.substring(0,2));

            // const objectKey = "v" + diff;
            // resultDoc[objectKey] = {}

        });


        docs.forEach( doc => {
            if (commonUtil.isEmpty(doc)) return;
            const fWeather = commonUtil.removeMissValue(doc.Item);
            const diff = commonUtil.getDayDiff(fWeather.id.substring(0, 8), yyyymmdd);
            const objectKey = "ta" + diff;
            resultDoc[objectKey] = {}
            
            const maxKey = "taMax" + diff;
            const minKey = "taMin" + diff;
            
            if (fWeather[maxKey] && fWeather[minKey] && curWeather.T1H
                && fWeather[maxKey] < 900 && fWeather[maxKey] > -900 
                && fWeather[minKey] < 900 && fWeather[minKey] > -900 
                && curWeather.T1H < 900 && curWeather.T1H > -900
            ) {
                const t1h = (() => {
                    if (fWeather[maxKey] < curWeather.T1H){
                        return ((+curWeather.T1H) - (+fWeather[maxKey])).toFixed(1);
                    }
                    else if (fWeather[minKey] > curWeather.T1H) {
                        return ((+curWeather.T1H) - (+fWeather[minKey])).toFixed(1);
                    }
                    return 0;
                })();
                Object.assign( resultDoc[objectKey], {
                    ["ta"] : t1h,
                    ["ta" + diff + "_d"] : {
                        "min" : fWeather[minKey],
                        "max" : fWeather[maxKey],
                        "a" : curWeather.PTY,
                    }
                });
            }
        });
        resolve(resultDoc);
    });
});


// 하루에 한번만 호출하도록..
module.exports.getLandFDocs  = async(event, map_info) => new Promise((resolve, reject) => {
    const {yyyymmdd, hour} = commonUtil.getDateTime(event);
    const dateTimes = getMidStatsHistoryTimes(event);
    
    const curWeatherKeys = [
        commonUtil.getYMDandHour( commonUtil.getDate(event, -1)),
        commonUtil.getYMDandHour( commonUtil.getDate(event, -2)),
        commonUtil.getYMDandHour( commonUtil.getDate(event, -3)),
        commonUtil.getYMDandHour( commonUtil.getDate(event, -4)),
        commonUtil.getYMDandHour( commonUtil.getDate(event, -5)),
        commonUtil.getYMDandHour( commonUtil.getDate(event, -6)),
        commonUtil.getYMDandHour( commonUtil.getDate(event, -7)),
        commonUtil.getYMDandHour( commonUtil.getDate(event, -8)),
        commonUtil.getYMDandHour( commonUtil.getDate(event, -9)),
        commonUtil.getYMDandHour( commonUtil.getDate(event, -10)),
        commonUtil.getYMDandHour( commonUtil.getDate(event, -11)),
        commonUtil.getYMDandHour( commonUtil.getDate(event, -12)),
    ];
    const curWeatherPromises = curWeatherKeys.map( tmpYMDH => {
        return commonUtil.getData("ultraSrtNcst", tmpYMDH.yyyymmdd + "." + tmpYMDH.hour + "00." + map_info.x + "." + map_info.y);
    });

    Promise.all(curWeatherPromises).then(rtnDatas => {
        if (rtnDatas.length === 0) {
            console.error("There is no current weather data in ultraSrtNcst. key is ", curWeatherKey);
            return ;
        }
        const curWeathers = rtnDatas.map(rtnData => {
            return commonUtil.removeMissValue(rtnData.Item)
        }).filter(c=>c);
        const getDocsPromises = dateTimes.map(dateTime => {
            const key = dateTime.yyyymmdd + "." + dateTime.hour + "00." + map_info.midLand;
            return commonUtil.getData("midLandFcst", key);
        });

        let resultDoc = {};
        Promise.all(getDocsPromises).then(docs => {
            docs.forEach( doc => {
                if (commonUtil.isEmpty(doc)) return;
                const fWeather = commonUtil.removeMissValue(doc.Item);
                const diff = Math.abs(commonUtil.getDayDiff(fWeather.id.substring(0, 8), yyyymmdd));
                const objectKey = "d" + diff;
                resultDoc[objectKey] = {}
                
                const mainKey = (() => {
                    if (diff <= 7) {
                        // if(hour <= 11) { //0~11
                        //     return diff + "Am";
                        // }
                        // return diff + "Pm";
                        return diff + "Pm";
                    }
                    else if (diff > 7) {
                        return diff;
                    }
                })();

                if (fWeather["rnSt" + mainKey] && (curWeathers.some(c=> c && 'RN1' in c) || curWeathers.some(c=> c && 'PTY' in c))) {
                    Object.assign( resultDoc[objectKey], {
                        "rnSt_e" : fWeather["rnSt" + mainKey],
                        "wf_e" : fWeather["wf" + mainKey],
                        "rn1_a" : curWeathers.filter(c=>c.RN1).map(c=>c.RN1),
                        "tpy_a" : curWeathers.filter(c=>c.PTY).map(c=>c.PTY),
                    });
                }
            });
            console.log( "resultDoc", resultDoc);
            resolve(resultDoc);
        });
    });
});