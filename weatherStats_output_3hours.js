'use strict';
const weatherUtil = require('./module/weather_util');
const commonUtil = require('./module/common_util');
const config = require('./config');

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
    const {yyyymmdd, hour} = commonUtil.getDateTime(event, -1);
    const statsDocsPromises = config.map_infos.map( (map_info, index) => new Promise((resolve, reject) => {
        try {
            delay(index * 3000).then(() => {
                console.log( "delay 3000");
                const docId = "lastest-" + map_info.id;
                commonUtil.getFromS3(docId + ".json").then(localDoc => {
                //commonUtil.getData("weather-output", docId).then(localDoc => {
                    get3hourlyDoc(event, map_info, localDoc).then(doc => {
                        doc["id"] = docId;
                        doc["local"] = map_info.id;
                        doc["update"] = new Date().getTime();
                        commonUtil.uploadToS3(docId + ".json", doc).then( data => {
                            console.log( "uploaded to s3")
                            resolve(doc);
                        });
                    }).catch(error => reject(error.message));
                });
            });
        }
        catch(err) {
            reject(err);
        }
    }));
    
    Promise.allSettled(statsDocsPromises).then( datas => {
        const docs = datas.filter( d=> d.status === 'fulfilled').map( d=> d.value).flat();
        commonUtil.sendtoSlack("Update weather-output 3hourly : " + docs.length).then( () => {
            if( docs.length === 0) {
                callback(null, response("no data to update"));
                return ;
            }
            commonUtil.getFromS3("lastest.json").then(totalDoc => {
                get3hourlyTotalDoc(event, docs, totalDoc).then(doc => {
                    // total doc
                    doc["id"] = "lastest";
                    doc["update"] = new Date().getTime();
                    
                    commonUtil.uploadToS3("lastest.json", doc).then( data => {
                    //importData("weather-output", [doc]).then( count => {
                        commonUtil.sendtoSlack("Update weather-output 3hourly: lastest").then( () => {
                            callback(null, response(" weather-output done"));
                        });
                    });
                })
            });
        });
    }).catch(err=>{
        console.error( "err", err);
        callback(null, response("err" + err));
    });
};
const get3hourlyTotalDoc = async(event, docs, totalDoc) => new Promise((resolve, reject) => {
    const {yyyymmdd, hour} = commonUtil.getDateTime(event, -1);
    const timeKeys = ['v6','v9','v12','v15','v18','v21','v24','v36','v48'];
    for(const index in timeKeys) {
        const timeKey = timeKeys[index];
        if( !(timeKey in totalDoc)) {
            totalDoc[timeKey] = {
                'pty' : {'avg' : -1, 'info': {}}, // 날씨
                'pop_0'  :  {'avg' : -1, 'info': {}},
                'pop_10' :  {'avg' : -1, 'info': {}},
                'pop_20' :  {'avg' : -1, 'info': {}},
                'pop_30' :  {'avg' : -1, 'info': {}},
                'pop_40' :  {'avg' : -1, 'info': {}},
                'pop_50' :  {'avg' : -1, 'info': {}},
                'pop_60' :  {'avg' : -1, 'info': {}},
                'pop_70' :  {'avg' : -1, 'info': {}},
                'pop_80' :  {'avg' : -1, 'info': {}},
                'pop_90' :  {'avg' : -1, 'info': {}},// 강수 확률 
                'reh' : {'history' : [],'avg' : 0, 'info': {}}, // 습도 
                'vec' : {'history' : [],'avg' : 0, 'info': {}}, // 풍향
                't1h' : {'history' : [],'avg' : 0, 'info': {}}, // 온도
                'rn1' : {'history' : [],'avg' : 0, 'info': {}}, // 강수량
                'wsd' : {'history' : [],'avg' : 0, 'info': {}}, // 풍량
                'stmp_w' : {'history' : [],'avg' : 0, 'info': {}}, // 체감온도 겨울
                'stmp_s' : {'history' : [],'avg' : 0, 'info': {}}, // 체감온도 여름
            }
        }
        const infoKeys1 = [
            'reh','vec','t1h','rn1','wsd','stmp_w','stmp_s'
        ];
        for(const infoIndex in infoKeys1) {
            const infoKey = infoKeys1[infoIndex];
        
            let tmpInfo = {};
            let tmpSum = 0;
            docs.forEach( doc=> {
                if ( timeKey in doc && infoKey in doc[timeKey]) {
                    tmpInfo[doc.local] = +(doc[timeKey][infoKey]['avg']);
                    tmpSum += +(doc[timeKey][infoKey]['avg']);
                }
            });
            totalDoc[timeKey][infoKey]["avg"] = (tmpSum / docs.length).toFixed(2);
            totalDoc[timeKey][infoKey]["info"] = tmpInfo;
            totalDoc[timeKey][infoKey]['history'] = commonUtil.addItemByTime(totalDoc[timeKey][infoKey]['history'], {
                "avg" : totalDoc[timeKey][infoKey]["avg"],
                't' : commonUtil.convertYMDHtoDate(yyyymmdd, hour).getTime(),
            }, LOCAL_HISTORY_LENGTH);
        }

        const infoKeys2 = [
            'pty', 'pop_0', 'pop_10', 'pop_20', 'pop_30', 'pop_40', 'pop_50', 'pop_60', 'pop_70', 'pop_80', 'pop_90', 
        ];
        for(const infoIndex in infoKeys2) {
            const infoKey = infoKeys2[infoIndex];
        
            let tmpInfo = {};
            let tmpSum = 0;
            let tmpCount = 0;
            docs.forEach( doc=> {
                if ( timeKey in doc && infoKey in doc[timeKey] && doc[timeKey][infoKey]['rate'] >= 0) {
                    tmpInfo[doc.local] = +(doc[timeKey][infoKey]['rate']);
                    tmpSum += +(doc[timeKey][infoKey]['rate']);
                    tmpCount += 1;
                }
            });
            if(tmpCount>0){
                totalDoc[timeKey][infoKey]["avg"] = (tmpSum / tmpCount).toFixed(2);
                totalDoc[timeKey][infoKey]["info"] = tmpInfo;
            }
        }
        resolve(totalDoc);
    }
});
const get3hourlyDoc = async(event, map_info,  localDoc) => new Promise((resolve, reject) => {
    const {yyyymmdd, hour} = commonUtil.getDateTime(event, -1);
    const hourlyStatsKey = "3hourly-" + yyyymmdd + "." + hour + "00." + map_info.x + "." + map_info.y;

    commonUtil.getData("weather-hourly-stats", hourlyStatsKey).then(rtnData => {
        if (commonUtil.isEmpty(rtnData.Item)) {
            console.error("There is no data in weather-hourly-stats. key is ", hourlyStatsKey);
            reject("There is no data in weather-hourly-stats. key is ", hourlyStatsKey);
            return ;
        }
        const hourlyStats = rtnData.Item;
        //const timeKeys = ['v6','v9','v12','v15','v18','v21','v24','v27','v30','v33','v36','v39','v42','v45','v48'];
        const timeKeys = ['v6','v9','v12','v15','v18','v21','v24','v36','v48'];
        for(const index in timeKeys) {
            const timeKey = timeKeys[index];
            if (! (timeKey in hourlyStats)) continue;
            if( !(timeKey in localDoc)) {
                localDoc[timeKey] = {
                    'pty' : {'raw':[], 'history' : [], 'rate' : -1}, // 날씨
                    'pop_0'  : { 'raw':[], 'history' : [], 'rate' : -1 },
                    'pop_10' : { 'raw':[], 'history' : [], 'rate' : -1 },
                    'pop_20' : { 'raw':[], 'history' : [], 'rate' : -1 },
                    'pop_30' : { 'raw':[], 'history' : [], 'rate' : -1 },
                    'pop_40' : { 'raw':[], 'history' : [], 'rate' : -1 },
                    'pop_50' : { 'raw':[], 'history' : [], 'rate' : -1 },
                    'pop_60' : { 'raw':[], 'history' : [], 'rate' : -1 },
                    'pop_70' : { 'raw':[], 'history' : [], 'rate' : -1 },
                    'pop_80' : { 'raw':[], 'history' : [], 'rate' : -1 },
                    'pop_90' : { 'raw':[], 'history' : [], 'rate' : -1 },// 강수 확률 
                    'reh' : {'raw':[], 'history' : [], 'avg' : 0}, // 습도 
                    'vec' : {'raw':[], 'history' : [], 'avg' : 0}, // 풍향
                    't1h' : {'raw':[], 'history' : [], 'avg' : 0}, // 온도
                    'rn1' : {'raw':[], 'history' : [], 'avg' : 0}, // 강수량
                    'wsd' : {'raw':[], 'history' : [], 'avg' : 0}, // 풍량
                    'stmp_w' : {'raw':[], 'history' : [],'avg' : 0}, // 체감온도 겨울
                    'stmp_s' : {'raw':[], 'history' : [],'avg' : 0}, // 체감온도 여름
                }
            }
            if('pty' in hourlyStats[timeKey] && 'pty_d' in hourlyStats[timeKey] && 
                (hourlyStats[timeKey]['pty_d']['e'] !== '0' || hourlyStats[timeKey]['pty_d']['a'].some(c => c && c !== '0'))) {
                const tmpObj = {
                    't' : commonUtil.convertYMDHtoDate(yyyymmdd, hour).getTime(),
                    'e' : hourlyStats[timeKey]['pty_d']['e'],
                    'a' : hourlyStats[timeKey]['pty_d']['a'],
                    'f': hourlyStats[timeKey]['pty'] === 'T'
                };
                localDoc[timeKey]['pty']['raw'] = commonUtil.addItemByTime(localDoc[timeKey]['pty']['raw'], tmpObj, LOCAL_HISTORY_LENGTH);
                localDoc[timeKey]['pty']["rate"] = (() => {
                    return localDoc[timeKey]['pty']['raw'].filter( obj=> obj.f).length / localDoc[timeKey]['pty']['raw'].length;
                })();
                localDoc[timeKey]['pty']['history'] = commonUtil.addItemByTime(localDoc[timeKey]['pty']['history'], {
                    't' : commonUtil.convertYMDHtoDate(yyyymmdd, hour).getTime(),
                    'r' : localDoc[timeKey]['pty']["rate"]
                }, LOCAL_HISTORY_LENGTH);

                // 강수 확률 
                if ( !( 'pop_' + hourlyStats[timeKey]['pty_d']['e_pop'] in localDoc[timeKey])) {
                    localDoc[timeKey]['pop_' + hourlyStats[timeKey]['pty_d']['e_pop']] =  { 'raw':[], 'history' : [], 'rate' : 0 };
                }
                localDoc[timeKey]['pop_' + hourlyStats[timeKey]['pty_d']['e_pop']]["raw"] = commonUtil.addItemByTime(localDoc[timeKey]['pop_' + hourlyStats[timeKey]['pty_d']['e_pop']]["raw"], {
                    't' : commonUtil.convertYMDHtoDate(yyyymmdd, hour).getTime(),
                    'f' : hourlyStats[timeKey]['pty_d']['a'].some(c => c !== '0')
                }, LOCAL_HISTORY_LENGTH);
                localDoc[timeKey]['pop_' + hourlyStats[timeKey]['pty_d']['e_pop']]["rate"] = (() => {
                    return localDoc[timeKey]['pop_' + hourlyStats[timeKey]['pty_d']['e_pop']]["raw"].filter( obj=> obj.f).length / localDoc[timeKey]['pop_' + hourlyStats[timeKey]['pty_d']['e_pop']]["raw"].length;
                })();
                
                localDoc[timeKey]['pop_' + hourlyStats[timeKey]['pty_d']['e_pop']]["history"] = commonUtil.addItemByTime(localDoc[timeKey]['pop_' + hourlyStats[timeKey]['pty_d']['e_pop']]["history"], {
                    't' : commonUtil.convertYMDHtoDate(yyyymmdd, hour).getTime(),
                    'r' : localDoc[timeKey]['pop_' + hourlyStats[timeKey]['pty_d']['e_pop']]["rate"]
                }, LOCAL_HISTORY_LENGTH);
            }


            if('reh' in hourlyStats[timeKey] && 'reh_d' in hourlyStats[timeKey]) {
                const tmpObj = {
                    't' : commonUtil.convertYMDHtoDate(yyyymmdd, hour).getTime(),
                    'e' : hourlyStats[timeKey]['reh_d']['e'],
                    'a' : hourlyStats[timeKey]['reh_d']['a'],
                    'd': hourlyStats[timeKey]['reh']
                };
                localDoc[timeKey]['reh']['raw'] = commonUtil.addItemByTime(localDoc[timeKey]['reh']['raw'], tmpObj, LOCAL_HISTORY_LENGTH);

                localDoc[timeKey]['reh']["avg"] = (() => {
                    return (localDoc[timeKey]['reh']['raw'].reduce((a, b) => a + (Math.abs(+b.d) || 0), 0) / localDoc[timeKey]['reh']['raw'].length).toFixed(2);
                })();
                localDoc[timeKey]['reh']['history'] = commonUtil.addItemByTime(localDoc[timeKey]['reh']['history'], {
                    't' : commonUtil.convertYMDHtoDate(yyyymmdd, hour).getTime(),
                    'a' : localDoc[timeKey]['reh']["avg"]
                }, LOCAL_HISTORY_LENGTH);
            }

            if('vec' in hourlyStats[timeKey] && 'vec_d' in hourlyStats[timeKey]) {
                const tmpObj = {
                    't' : commonUtil.convertYMDHtoDate(yyyymmdd, hour).getTime(),
                    'e' : hourlyStats[timeKey]['vec_d']['e'],
                    'a' : hourlyStats[timeKey]['vec_d']['a'],
                    'd': hourlyStats[timeKey]['vec']
                };
                localDoc[timeKey]['vec']['raw'] = commonUtil.addItemByTime(localDoc[timeKey]['vec']['raw'], tmpObj, LOCAL_HISTORY_LENGTH);

                localDoc[timeKey]['vec']["avg"] = (() => {
                    return (localDoc[timeKey]['vec']['raw'].reduce((a, b) => a + (+b.d || 0), 0) / localDoc[timeKey]['vec']['raw'].length).toFixed(2);
                })();
                localDoc[timeKey]['vec']['history'] = commonUtil.addItemByTime(localDoc[timeKey]['vec']['history'], {
                    't' : commonUtil.convertYMDHtoDate(yyyymmdd, hour).getTime(),
                    'a' : localDoc[timeKey]['vec']["avg"]
                }, LOCAL_HISTORY_LENGTH);
            }

            if('t1h' in hourlyStats[timeKey] && 't1h_d' in hourlyStats[timeKey]) {
                const tmpObj = {
                    't' : commonUtil.convertYMDHtoDate(yyyymmdd, hour).getTime(),
                    'e' : hourlyStats[timeKey]['t1h_d']['e'],
                    'a' : hourlyStats[timeKey]['t1h_d']['a'],
                    'd': hourlyStats[timeKey]['t1h']
                };
                localDoc[timeKey]['t1h']['raw'] = commonUtil.addItemByTime(localDoc[timeKey]['t1h']['raw'], tmpObj, LOCAL_HISTORY_LENGTH);
                
                localDoc[timeKey]['t1h']["avg"] = (() => {
                    return (localDoc[timeKey]['t1h']['raw'].reduce((a, b) => a + (Math.abs(+b.d) || 0), 0) / localDoc[timeKey]['t1h']['raw'].length).toFixed(2);
                })();
                localDoc[timeKey]['t1h']['history'] = commonUtil.addItemByTime(localDoc[timeKey]['t1h']['history'], {
                    't' : commonUtil.convertYMDHtoDate(yyyymmdd, hour).getTime(),
                    'a' : localDoc[timeKey]['t1h']["avg"]
                }, LOCAL_HISTORY_LENGTH);
            }
            
            if('tn1' in hourlyStats[timeKey] && 'tn1_d' in hourlyStats[timeKey]) {
                if (hourlyStats[timeKey]['tn1_d']['e']!=='1mm 미만' || 
                    hourlyStats[timeKey]['tn1_d']['a']!=='0') {
                        const tmpObj = {
                        't' : commonUtil.convertYMDHtoDate(yyyymmdd, hour).getTime(),
                        'e' : hourlyStats[timeKey]['tn1_d']['e'],
                        'a' : hourlyStats[timeKey]['tn1_d']['a'],
                        'd': hourlyStats[timeKey]['tn1']
                    };
                    localDoc[timeKey]['rn1']['raw'] = commonUtil.addItemByTime(localDoc[timeKey]['rn1']['raw'], tmpObj, LOCAL_HISTORY_LENGTH);

                    localDoc[timeKey]['rn1']["avg"] = (() => {
                        return (localDoc[timeKey]['rn1']['raw'].reduce((a, b) => a + (Math.abs(+b.d) || 0), 0) / localDoc[timeKey]['rn1']['raw'].length).toFixed(2);
                    })();
                    localDoc[timeKey]['rn1']['history'] = commonUtil.addItemByTime(localDoc[timeKey]['rn1']['history'], {
                        't' : commonUtil.convertYMDHtoDate(yyyymmdd, hour).getTime(),
                        'a' : localDoc[timeKey]['rn1']["avg"]
                    }, LOCAL_HISTORY_LENGTH);
                }
            }
            if('wsd' in hourlyStats[timeKey] && 'wsd_d' in hourlyStats[timeKey]) {
                const tmpObj = {
                    't' : commonUtil.convertYMDHtoDate(yyyymmdd, hour).getTime(),
                    'e' : hourlyStats[timeKey]['wsd_d']['e'],
                    'a' : hourlyStats[timeKey]['wsd_d']['a'],
                    'd': hourlyStats[timeKey]['wsd']
                };
                localDoc[timeKey]['wsd']['raw'] = commonUtil.addItemByTime(localDoc[timeKey]['wsd']['raw'], tmpObj, LOCAL_HISTORY_LENGTH);

                localDoc[timeKey]['wsd']["avg"] = (() => {
                    return (localDoc[timeKey]['wsd']['raw'].reduce((a, b) => a + (Math.abs(+b.d) || 0), 0) / localDoc[timeKey]['wsd']['raw'].length).toFixed(2);
                })();
                localDoc[timeKey]['wsd']['history'] = commonUtil.addItemByTime(localDoc[timeKey]['wsd']['history'], {
                    't' : commonUtil.convertYMDHtoDate(yyyymmdd, hour).getTime(),
                    'a' : localDoc[timeKey]['wsd']["avg"]
                }, LOCAL_HISTORY_LENGTH);
            }
            
            function getSensTemp_w(t, v) {
                return 13.12 + 0.6215*t - 11.37 * Math.pow(v, 0.16) + 0.3965 * Math.pow(v, 0.16) * t;
            }
            // 체감 온도 ( 1,2,3,4,5, 10,11,12 월용)
            if( 'wsd' in hourlyStats[timeKey] && 'wsd_d' in hourlyStats[timeKey] &&
                't1h' in hourlyStats[timeKey] && 't1h_d' in hourlyStats[timeKey]) {

                let actualT1h = 0;
                let actualT1hDiff = 0;
                hourlyStats[timeKey]['t1h_d']['a'].forEach( t1h=> {
                    if( actualT1hDiff <= Math.abs(hourlyStats[timeKey]['t1h_d']['e'] - t1h)) {
                        actualT1h = t1h;
                    }
                });

                let actualWsd = 0;
                let actualWsdDiff = 0;
                hourlyStats[timeKey]['wsd_d']['a'].forEach( wsd=> {
                    if( actualWsdDiff <= Math.abs(hourlyStats[timeKey]['wsd_d']['e'] - wsd)) {
                        actualWsd = wsd;
                    }
                });

                const eTemp = getSensTemp_w(hourlyStats[timeKey]['t1h_d']['e'],hourlyStats[timeKey]['wsd_d']['e']).toFixed(2);
                const aTemp = getSensTemp_w(actualT1h,actualWsd).toFixed(2);
                const tmpObj = {
                    't' : commonUtil.convertYMDHtoDate(yyyymmdd, hour).getTime(),
                    'e' : eTemp,
                    'a' : aTemp,
                    'd': (aTemp - eTemp).toFixed(2),
                };
                localDoc[timeKey]['stmp_w']['raw'] = commonUtil.addItemByTime(localDoc[timeKey]['stmp_w']['raw'], tmpObj, LOCAL_HISTORY_LENGTH);
                
                localDoc[timeKey]['stmp_w']["avg"] = (() => {
                    return (localDoc[timeKey]['stmp_w']['raw'].reduce((a, b) => a + (Math.abs(b.d) || 0), 0) / localDoc[timeKey]['stmp_w']['raw'].length).toFixed(2);
                })();
                localDoc[timeKey]['stmp_w']['history'] = commonUtil.addItemByTime(localDoc[timeKey]['stmp_w']['history'], {
                    't' : commonUtil.convertYMDHtoDate(yyyymmdd, hour).getTime(),
                    'a' : localDoc[timeKey]['stmp_w']["avg"]
                }, LOCAL_HISTORY_LENGTH);
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
                
                let actualT1h = 0;
                let actualT1hDiff = 0;
                hourlyStats[timeKey]['t1h_d']['a'].forEach( t1h=> {
                    if( actualT1hDiff <= Math.abs(hourlyStats[timeKey]['t1h_d']['e'] - t1h)) {
                        actualT1h = t1h;
                    }
                });

                let actualReh = 0;
                let actualRehDiff = 0;
                hourlyStats[timeKey]['reh_d']['a'].forEach( reh=> {
                    if( actualRehDiff <= Math.abs(hourlyStats[timeKey]['reh_d']['e'] - reh)) {
                        actualReh = reh;
                    }
                });

                const eTemp = getSensTemp_s(hourlyStats[timeKey]['t1h_d']['e'],hourlyStats[timeKey]['reh_d']['e']).toFixed(2);
                const aTemp = getSensTemp_s(actualT1h,actualReh).toFixed(2);
                const tmpObj = {
                    't' : commonUtil.convertYMDHtoDate(yyyymmdd, hour).getTime(),
                    'e' : eTemp,
                    'a' : aTemp,
                    'd': (aTemp - eTemp).toFixed(2),
                };
                localDoc[timeKey]['stmp_s']['raw'] = commonUtil.addItemByTime(localDoc[timeKey]['stmp_s']['raw'], tmpObj, LOCAL_HISTORY_LENGTH);
                
                localDoc[timeKey]['stmp_s']["avg"] = (() => {
                    return (localDoc[timeKey]['stmp_s']['raw'].reduce((a, b) => a + (Math.abs(b.d) || 0), 0) / localDoc[timeKey]['stmp_s']['raw'].length).toFixed(2);
                })();
                localDoc[timeKey]['stmp_s']['history'] = commonUtil.addItemByTime(localDoc[timeKey]['stmp_s']['history'], {
                    't' : commonUtil.convertYMDHtoDate(yyyymmdd, hour).getTime(),
                    'a' : localDoc[timeKey]['stmp_s']["avg"]
                }, LOCAL_HISTORY_LENGTH);
            }
        }
        resolve(localDoc);
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
