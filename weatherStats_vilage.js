
'use strict';
const commonUtil = require('./module/common_util');


// const getDateTime3 = (event) => {
//     var today = new Date();
//     var hour = 99;

//     if(event.queryStringParameters && !isNaN(event.queryStringParameters.hour)) {
//         hour = +event.queryStringParameters.hour;
//         console.log("[SET] Custom hour '" + hour + "' set.");
//     }
//     else {
//         today.setHours(today.getHours() + 9);
//         hour = today.getHours();
//     }
    
//     let rtnDateTimes = [];
//     for(i=-1;i<=1;i++) {
//         const tmpDay = new Date(today);
//         tmpDay.setHours(tmpDay.getHours() + i);

//         var dd = tmpDay.getDate();
//         var mm = tmpDay.getMonth()+1;
//         var yyyy = tmpDay.getFullYear();
      
//         if(hour<10) {
//             hour='0'+hour;
//         }
//         if(mm<10) {
//             mm='0'+mm;
//         }
//         if(dd<10) {
//             dd='0'+dd;
//         } 
//         const yyyymmdd = yyyy + "" + mm + "" + dd;
//         rtnDateTimes.push({yyyymmdd, hour});
//     }
//     return rtnDateTimes;
// }

const getVilageStatsHistoryTimes = (event) => {
    let tmpDay = commonUtil.getDate(event, -1); // 기준 시간

    var dateTimes = [];
    var fromTmpDay = new Date( tmpDay.getTime() - (48*60*60*1000));
    while(tmpDay >= fromTmpDay) {
        const {yyyymmdd, hour} = commonUtil.getYMDandHour(tmpDay);
        dateTimes.push( {yyyymmdd, hour});
        tmpDay.setTime(tmpDay.getTime() - (3 * 60 * 60 * 1000));
    };
    return dateTimes;
}

module.exports.getDocs  = async(event, map_info) => new Promise((resolve, reject) => {
    const {yyyymmdd, hour} = commonUtil.getDateTime(event, -1);
    const dateTimes = getVilageStatsHistoryTimes(event);
    

    const curWeatherKeys = [
        commonUtil.getYMDandHour( commonUtil.getDate(event, 0)),
        commonUtil.getYMDandHour( commonUtil.getDate(event, -1)),
        commonUtil.getYMDandHour( commonUtil.getDate(event, -2)),
    ];
    console.log( "curWeatherKeys", curWeatherKeys);
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
        console.log( "curWeathers", curWeathers);
        const getDocsPromises = dateTimes.map(dateTime => {
            const key = dateTime.yyyymmdd + "." + dateTime.hour + "00." + yyyymmdd + "." + hour + "00." + map_info.x + "." + map_info.y;
            return commonUtil.getData("vilageFcst", key);
        });

        let resultDoc = {};
        Promise.all(getDocsPromises).then(docs => {
            docs.forEach( doc => {
                if (commonUtil.isEmpty(doc)) return;
                const fWeather = commonUtil.removeMissValue(doc.Item);
                const diff = commonUtil.getHourDiff(fWeather.baseDate, fWeather.baseTime.substring(0,2), fWeather.fcstDate, fWeather.fcstTime.substring(0,2));

                const objectKey = "v" + diff;
                resultDoc[objectKey] = {}

                if (fWeather.PTY && curWeathers.some(c=> c && 'PTY' in c)) {
                    const pty = (() => {
                        // 맑음
                        if(fWeather.PTY==="0") {
                            return curWeathers.every(c=> !('PTY' in c) || c.PTY=="0");
                        }
                        // 비
                        if(fWeather.PTY==="1") {
                            return curWeathers.every(c=> !('PTY' in c) || ["1", "2", "4"].includes(c.PTY));
                        }
                        // 눈/비
                        if(fWeather.PTY==="2") {
                            return curWeathers.every(c=> !('PTY' in c) || ["1", "2", "3", "4"].includes(c.PTY));
                        }
                        // 눈
                        if(fWeather.PTY==="3") {
                            return curWeathers.every(c=> !('PTY' in c) || ["3"].includes(c.PTY));
                        }
                        // 빗방울
                        if(fWeather.PTY==="5") {
                            return curWeathers.every(c=> !('PTY' in c) || ["1"].includes(c.PTY));
                        }
                        // 빗방울눈날림
                        if(fWeather.PTY==="6") {
                            return curWeathers.every(c=> !('PTY' in c) || ["1", "2", "3"].includes(c.PTY));
                        }
                        // 눈날림
                        if(fWeather.PTY==="7") {
                            return curWeathers.every(c=> !('PTY' in c) || ["3"].includes(c.PTY));
                        }
                        return "F";
                    })();
                    Object.assign( resultDoc[objectKey], {
                        "pty" : pty? "T" : "F",
                        "pty_d" : {
                            "e" : fWeather.PTY,
                            "e_pop" : fWeather.POP,
                            "a" : curWeathers.map(c=>c.PTY),
                        }
                    });
                }

                if (fWeather.TMP && curWeathers.some(c=> c && 'T1H' in c)) {
                    const maxT1H = Math.max.apply(null, curWeathers.map(c=> {
                        if ('T1H' in c) {
                            return ((+c.T1H) - (+fWeather.TMP)).toFixed(1)
                        }
                        return 0;
                    }).map(Math.abs));
                    Object.assign( resultDoc[objectKey], {
                        "t1h" : maxT1H, // 기온
                        "t1h_d" : {
                            "e" : fWeather.TMP,
                            "a" : curWeathers.map(c=>c.T1H),
                        }
                    });
                }

                if (fWeather.PCP && curWeathers.some(c=> c && 'RN1' in c)) {
                    const tn1 = Math.max.apply(null, curWeathers.map(c=> {
                        if (fWeather.PCP == "1mm 미만") {
                            return (+c.RN1).toFixed(1);
                        }
                        else if (fWeather.PCP == "30~50mm") {
                            if (+c.RN1 <= 30) {
                                return ((+c.RN1) - 30).toFixed(1);
                            }
                            else if (+c.RN1 >= 50) {
                                return ((+c.RN1) - 50).toFixed(1);
                            }
                            // 30 ~ 50
                            return 0;
                        }
                        else if (fWeather.PCP == "50mm 이상") {
                            if (+c.RN1 < 50) {
                                return ((+c.RN1) - 50).toFixed(1);
                            }
                            return 0;
                        }
                        else if (fWeather.PCP == "강수없음") {
                            if (+c.RN1 !== 0) {
                                return (+c.RN1).toFixed(1);
                            }
                            return 0;
                        }
                        else { // 1 ~ 29
                            return ((+c.RN1) - (+(fWeather.PCP.slice(0, -2)))).toFixed(1);
                        }
                    }).map(Math.abs));
                    Object.assign( resultDoc[objectKey], {
                        "tn1" : tn1, // 강수량
                        "tn1_d" : {
                            "e" : fWeather.PCP,
                            "a" : curWeathers.map(c=>c.RN1),
                        }
                    });
                }

                if (fWeather.REH && curWeathers.some(c=> c && 'REH' in c)) {
                    const maxREH = Math.max.apply(null, curWeathers.map(c=> {
                        if ('REH' in c) {
                            return ((+c.REH) - (+fWeather.TMP)).toFixed(1)
                        }
                        return 0;
                    }).map(Math.abs));
                    Object.assign( resultDoc[objectKey], {
                        "reh" : maxREH, // 습도 
                        "reh_d" : {
                            "e" : fWeather.REH,
                            "a" : curWeathers.map(c=>c.REH),
                        },
                    });
                }

                if (fWeather.VEC && curWeathers.some(c=> c && 'VEC' in c)) {
                    const vec = Math.max.apply(null, curWeathers.map( c => {
                        if ('VEC' in c) {
                            // return Math.min(
                            //     Math.abs(Math.abs(fWeather.VEC) - Math.abs(c.VEC)),
                            //     Math.abs(Math.abs(fWeather.VEC - 360) - Math.abs(c.VEC)),
                            //     Math.abs(Math.abs(fWeather.VEC) - Math.abs(c.VEC - 360)),
                            // );
                            return Math.min(
                                Math.abs(fWeather.VEC - c.VEC),
                                Math.abs(c.VEC - fWeather.VEC),
                            );
                        }
                        return 0;
                    }).map(Math.abs));
                    Object.assign( resultDoc[objectKey], {
                        "vec" : vec, // 풍향 
                        "vec_d" : {
                            "e" : fWeather.VEC,
                            "a" : curWeathers.map(c=>c.VEC),
                        },
                    });
                }
                if (fWeather.WSD && curWeathers.some(c=> c && 'WSD' in c)) {
                    const wsd = Math.max.apply(null, curWeathers.map( c => {
                        if ('WSD' in c) {
                            return ((+fWeather.WSD)-(+c.WSD)).toFixed(1)
                        }
                        return 0;
                    }).map(Math.abs));

                    Object.assign( resultDoc[objectKey], {
                        "wsd" : wsd, // 풍속
                        "wsd_d" : {
                            "e" : fWeather.WSD,
                            "a" : curWeathers.map(c=>c.WSD),
                        }
                    });
                }
            });
            resolve(resultDoc);
        });
    });
});