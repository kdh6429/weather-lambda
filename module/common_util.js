
const AWS = require('aws-sdk'); 
const config = require('../config');

AWS.config.update(config.aws_remote_config);
const dynamoDb = new AWS.DynamoDB.DocumentClient();

// vilageFcst
function delay(interval) {
    return new Promise(resolve => setTimeout(resolve, interval));
}
const _insertData = (tableName, doc) => new Promise((resolve, reject) => {
    const params = {
        TableName: tableName,
        Item: JSON.parse(JSON.stringify(doc))
    }
    dynamoDb.put( params, function(err, data) {
        if (err) {
            reject("Unable to add item. Error JSON:", err);
        } else {
            //console.log("inserted" , params);
            resolve(true);
        }
    });
});

const _removeMissValue = (obj) => {
    for (const key in obj) {
        if ( !isNaN(obj[key]) 
            && !(["baseDate", "baseTime", "fcstDate", "fcstTime", "regId", "nx", "ny",].includes(key))
        ) {
            if ( +obj[key] > 900 || +obj[key] < -900) {
                console.log( key + " value is missing : " + obj[key]);
                obj[key] = null;
            }
        }
    }
    return obj;
}
const _queryData = (tableName, keys) => new Promise((resolve, reject) => {
    const params = {
        RequestItems: {
            [tableName]: {
                Keys: keys.map(key => { id: key }),
                //ConsistentRead: false, // optional (true | false)
            },
        },
    };
    dynamoDb.batchGet( params, function(err, data) {
        if (err) {
            reject("Unable to add item. Error JSON:", err);
        } else {
            resolve(data);
        }
    });
});

const _getDate = (event, addhour) => {
    var today = new Date();
    if(event.queryStringParameters && !isNaN(event.queryStringParameters.hour)) {
        hour = +event.queryStringParameters.hour;
        today.setHours(hour);
        console.log("[SET] Custom hour '" + hour + "' set.");
    }
    else {
        today.setHours(today.getHours() + 9);
        hour = today.getHours();
    }

    if(event.queryStringParameters && !isNaN(event.queryStringParameters.date)) {

        let date = event.queryStringParameters.date;
        today.setFullYear(+date.substring(0,4));
        today.setMonth((+date.substring(4,6))-1);
        today.setDate(+date.substring(6,8));

        console.log("[SET] Custom date '" + date + "' set.");
    }
    console.log( today);
    today.setHours( today.getHours() + addhour);
    console.log( today);
    return today;
}

module.exports = {
    genDBObjects: (items, keys, categoryKey, valueKey) => {
        var tmpObject = {};
        items.forEach(item => {
            const mainKey = keys.map(key=> item[key]).join(".");
            if(!(mainKey in tmpObject)) {
                tmpObject[mainKey] = { "id" : mainKey };
                tmpObject = _removeMissValue(tmpObject);
                keys.forEach(key=> {
                    tmpObject[mainKey][key] = item[key];
                })
            }
            tmpObject[mainKey][item[categoryKey]] = item[valueKey];
        });

        return Object.values(tmpObject);
    },
    importData: async (tableName, data) => new Promise((resolve, reject) => {
        try {
            Promise.all(data.map((doc, index) => {
                //return delay(index * 100).then(() => {
                    return _insertData(tableName, doc);
                //});
            })).then( rtnData => {
                resolve(rtnData.length);
            }).catch(err => {
                reject(err);
            });
        }
        catch(error) {
            console.error("[ERROR] Failed Importing data. ", error)
        }
    }),
    getData: (tableName, key) => new Promise((resolve, reject) => {
        const params = {
            TableName: tableName,
            Key : {
                'id' : key
            }
        };
        dynamoDb.get(params, function(err, data) {
            if (err) {
                console.log( "err", err);
                reject("Unable to query. Error JSON:", err);
            } else {
                resolve(data);
            }
        });
    }),
    getBatchData: async(tableName, keys) => new Promise((resolve, reject) => {
        const keyObjects = keys.map(key => { return {id: key} });
        const params = {
            RequestItems: {
                [tableName]: {
                    Keys: keyObjects,
                    //ConsistentRead: false, // optional (true | false)
                },
            },
        };
        dynamoDb.batchGet( params, function(err, data) {
            if (err) {
                console.error( "err", err);
                reject("Unable to query. Error JSON:", err);
            } else {
                resolve(data);
            }
        });
    }),
    isEmpty(object) {
        return Object.keys(object).length === 0;
    },
    getHourDiff:(fromYMD, fromHour, curYMD, curHour) => {
        var fYear = fromYMD.substring(0,4);
        var fMonth = fromYMD.substring(4,6);
        var fDate = fromYMD.substring(6,8);
        const fromDate = new Date(Number(fYear), Number(fMonth)-1, Number(fDate), fromHour);

        var cYear = curYMD.substring(0,4);
        var cMonth = curYMD.substring(4,6);
        var cDate = curYMD.substring(6,8);
        const curDate = new Date(Number(cYear), Number(cMonth)-1, Number(cDate), curHour);

        const diff = (curDate.getTime() - fromDate.getTime()) / 60 / 60 / 1000;
        return diff;
    },
    getDayDiff:(fromYMD, curYMD) => {
        var fYear = fromYMD.substring(0,4);
        var fMonth = fromYMD.substring(4,6);
        var fDate = fromYMD.substring(6,8);
        const fromDate = new Date(Number(fYear), Number(fMonth)-1, Number(fDate));

        var cYear = curYMD.substring(0,4);
        var cMonth = curYMD.substring(4,6);
        var cDate = curYMD.substring(6,8);
        const curDate = new Date(Number(cYear), Number(cMonth)-1, Number(cDate));

        const diff = (curDate.getTime() - fromDate.getTime()) / 24 / 60 / 60 / 1000;
        return diff;
    },
    getDate : (event, addhour=0) => {
        return _getDate (event, addhour);
    },
    getDateTime:(event, addhour=0) => {
        const today = _getDate(event, addhour);

        var hour = today.getHours();
        var dd = today.getDate();
        var mm = today.getMonth()+1;
        var yyyy = today.getFullYear();
    
        if(hour<10) {
            hour='0'+hour;
        }
        if(mm<10) {
            mm='0'+mm;
        }
        if(dd<10) {
            dd='0'+dd;
        } 
        const yyyymmdd = yyyy + "" + mm + "" + dd;
        console.log( "{yyyymmdd, hour}", {yyyymmdd, hour});
        return {yyyymmdd, hour}
    },
    getYMDandHour:(d) => {
        var dd = d.getDate();
        var mm = d.getMonth()+1;
        var hour = d.getHours() + "";
        var yyyy = d.getFullYear();
        if(hour<10) {
            hour='0'+hour;
        }
        if(mm<10) {
            mm='0'+mm;
        }
        if(dd<10) {
            dd='0'+dd;
        }
        const yyyymmdd = yyyy+""+mm+""+dd;
        return {yyyymmdd, hour};
    },
    removeMissValue:(obj) => {
        return _removeMissValue(obj)
    }
}
