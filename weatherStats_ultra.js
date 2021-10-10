
'use strict';
const commonUtil = require('./module/common_util');

const getUltraStatsHistoryTimes = (event) => {
    var curToday = new Date();
    var hour = 99;

    if(event.queryStringParameters && !isNaN(event.queryStringParameters.hour)) {
        hour = +event.queryStringParameters.hour;
        //console.log("[SET] Custom hour '" + hour + "' set.");
        curToday.setHours(hour);
    }
    else {
        curToday.setHours(curToday.getHours() + 9);
    }
    
    const tmpDay = new Date(curToday);
    var dateTimes = [];
    var fromTmpDay = new Date( tmpDay.getTime() - (6*60*60*1000));
    tmpDay.setTime(tmpDay.getTime() - (1 * 60 * 60 * 1000));
    while(tmpDay >= fromTmpDay) {
        const {yyyymmdd, hour} = commonUtil.getYMDandHour(tmpDay);
        dateTimes.push( {yyyymmdd, hour});

        tmpDay.setTime(tmpDay.getTime() - (1 * 60 * 60 * 1000));
    };
    return dateTimes;
}

module.exports.getDocs = async(event, map_info) => new Promise((resolve, reject) => {
    const {yyyymmdd, hour} = commonUtil.getDateTime(event);
    const dateTimes = getUltraStatsHistoryTimes(event);

    const curWeatherKey = yyyymmdd + "." + hour + "00." + map_info.x + "." + map_info.y;
    commonUtil.getData("ultraSrtNcst", curWeatherKey).then(rtnData => {
        if (commonUtil.isEmpty(rtnData)) {
            console.error("There is no current weather data in ultraSrtNcst. key is ", curWeatherKey);
            return ;
        }
        const curWeather = commonUtil.removeMissValue(rtnData.Item);

        const getDocsPromises = dateTimes.map(dateTime => {
            const key = dateTime.yyyymmdd + "." + dateTime.hour + "30." + yyyymmdd + "." + hour + "00." + map_info.x + "." + map_info.y;
            return commonUtil.getData("ultraSrtFcst", key);
        });

        let resultDoc = {};
        Promise.all(getDocsPromises).then(docs => {
            docs.forEach( doc => {
                if (commonUtil.isEmpty(doc)) return;
                const fWeather = commonUtil.removeMissValue(doc.Item);
                const diff = commonUtil.getHourDiff(fWeather.baseDate, fWeather.baseTime.substring(0,2), yyyymmdd, hour);

                const objectKey = "u" + diff;
                resultDoc[objectKey] = {}

                if (fWeather.PTY && curWeather.PTY) {
                    Object.assign( resultDoc[objectKey], {
                        "pty" : fWeather.PTY==curWeather.PTY? "T":"F",
                        "pty_d" : {
                            "e" : fWeather.PTY,
                            "a" : curWeather.PTY,
                        }
                    });
                }
                if (fWeather.T1H && curWeather.T1H) {
                    Object.assign( resultDoc[objectKey], {
                        "t1h" : ((+curWeather.T1H) - (+fWeather.T1H)).toFixed(1), // 기온
                        "t1h_d" : {
                            "e" : fWeather.T1H,
                            "a" : curWeather.T1H,
                        }
                    });
                }
                if (fWeather.RN1 && curWeather.RN1) {
                    const tn1 = (() => {
                        if (fWeather.RN1 == "1mm 미만") {
                            return (+curWeather.RN1).toFixed(1);
                        }
                        else if (fWeather.RN1 == "30~50mm") {
                            if (+curWeather.RN1 <= 30) {
                                return ((+curWeather.RN1) - 30).toFixed(1);
                            }
                            else if (+curWeather.RN1 >= 50) {
                                return ((+curWeather.RN1) - 50).toFixed(1);
                            }
                            // 30 ~ 50
                            return 0;
                        }
                        else if (fWeather.RN1 == "50mm 이상") {
                            if (+curWeather.RN1 < 50) {
                                return ((+curWeather.RN1) - 50).toFixed(1);
                            }
                            return 0;
                        }
                        else { // 1 ~ 29
                            return ((+curWeather.RN1) - (+(fWeather.RN1.slice(0, -2)))).toFixed(1);
                        }
                    })();
                    Object.assign( resultDoc[objectKey], {
                        "tn1" : tn1, // 강수량
                        "tn1_d" : {
                            "e" : fWeather.RN1,
                            "a" : curWeather.RN1,
                        }
                    });
                }
                if (fWeather.REH && curWeather.REH) {
                    Object.assign( resultDoc[objectKey], {
                        "reh" : (+curWeather.REH) - (+fWeather.REH), // 습도 
                        "reh_d" : {
                            "e" : fWeather.REH,
                            "a" : curWeather.REH,
                        }
                    });
                }
                if (fWeather.VEC && curWeather.VEC) {
                    const vec = (() => {
                        return Math.min(
                            Math.abs(fWeather.VEC - curWeather.VEC),
                            Math.abs(curWeather.VEC - fWeather.VEC),
                        );
                    })(); 
                    Object.assign( resultDoc[objectKey], {
                        "vec" : vec, // 풍향 
                        "vec_d" : {
                            "e" : fWeather.VEC,
                            "a" : curWeather.VEC,
                        }
                    });
                }
                if (fWeather.WSD && curWeather.WSD) {
                    Object.assign( resultDoc[objectKey], {
                        "wsd" : ((+curWeather.WSD)-(+fWeather.WSD)).toFixed(1), // 풍속
                        "wsd_d" : {
                            "e" : fWeather.WSD,
                            "a" : curWeather.WSD,
                        }
                    });
                }
            });
            resolve(resultDoc);
        });
    });
});
