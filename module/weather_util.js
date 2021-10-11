
const axios = require('axios');
const commonUtil = require('./common_util');

const SHORT_API_KEY = "LGm8psNdGFktTrxPchiklRQh/8Veo6mmeCrsYMIz11gKiPZSnzT2qvt7EgKxNwHlsKMsQFtuuqKr8vHhkD9YJQ==";
const MID_API_KEY = "LGm8psNdGFktTrxPchiklRQh/8Veo6mmeCrsYMIz11gKiPZSnzT2qvt7EgKxNwHlsKMsQFtuuqKr8vHhkD9YJQ==";

const axiosInstance = axios.create({
    timeout: 90000
});

module.exports = {
    getVilageFcst: (date, time, nx, ny) => new Promise((resolve, reject) => {
        _getVilageFcst(date, time, nx, ny).then(data => {
            if( !data.response || !data.response.header || data.response.header.resultCode !== "00") {
                reject("[ERROR] No Header or Result Code is not '00'", data);
            }
            //genDBObjects: (items, keys, categoryKey, valueKey)
            resolve(commonUtil.genDBObjects(data.response.body.items.item, ["baseDate", "baseTime", "fcstDate", "fcstTime", "nx", "ny"], "category", "fcstValue"));
        })
        .catch(err => {
            reject(err)
        })
    }),
    getUltraSrtNcst: (date, time, nx, ny) => new Promise((resolve, reject) => {
        _getUltraSrtNcst(date, time, nx, ny).then(data => {
            if( data.response.header.resultCode !== "00") {
                reject("[ERROR] Result Code is not '00'", data);
            }
            //genDBObjects: (items, keys, categoryKey, valueKey)
            resolve(commonUtil.genDBObjects(data.response.body.items.item, ["baseDate", "baseTime", "nx", "ny"], "category", "obsrValue"));
        })
        .catch(err => {
            reject(err)
        })
    }),
    getUltraSrtFcst: (date, time, nx, ny) => new Promise((resolve, reject) => {
        _getUltraSrtFcst(date, time, nx, ny).then(data => {
            if( data.response.header.resultCode !== "00") {
                reject("[ERROR] Result Code is not '00'", data);
            }
            //genDBObjects: (items, keys, categoryKey, valueKey)
            resolve(commonUtil.genDBObjects(data.response.body.items.item, ["baseDate", "baseTime", "fcstDate", "fcstTime", "nx", "ny"], "category", "fcstValue"));
        })
        .catch(err => {
            reject(err)
        })
    }),
    getMidTa: (date, time, regId) => new Promise((resolve, reject) => {
        _getMidTa(date, time, regId).then(data => {
            if( data.response.header.resultCode !== "00") {
                reject("[ERROR] Result Code is not '00'", data);
            }
            const item = data.response.body.items.item[0];
            item['id'] = date + "." + time + "." + regId
            //genDBObjects: (items, keys, categoryKey, valueKey)
            resolve([item]);
        })
        .catch(err => {
            reject(err)
        })
    }),
    getMidLandFcst: (date, time, regId) => new Promise((resolve, reject) => {
        _getMidLandFcst(date, time, regId).then(data => {
            if( data.response.header.resultCode !== "00") {
                reject("[ERROR] Result Code is not '00'", data);
            }
            const item = data.response.body.items.item[0];
            item['id'] = date + "." + time + "." + regId
            resolve([item]);
        })
        .catch(err => {
            reject(err)
        })
    })
    
}

const _getUltraSrtFcst = (date, time, nx, ny) => new Promise((resolve, reject) => {
    var path = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtFcst";
    path += "?base_date=" + date;
    path += "&base_time=" + time;
    path += "&nx=" + nx + "&ny=" + ny;
    path += "&pageNo=1&numOfRows=100";
    path += "&dataType=JSON";
    path += "&ServiceKey=" + SHORT_API_KEY;
    
    console.log( "path" , path);
    
    axiosInstance.get(path)
        .then(function (response) {
            resolve(response.data);
        }) .catch(function (error) { 
            reject(error)
        }) .then(function () { 
        });    
})


const _getUltraSrtNcst = (date, time, nx, ny) => new Promise((resolve, reject) => {

    var path = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst";
    path += "?base_date=" + date;
    path += "&base_time=" + time;
    path += "&nx=" + nx + "&ny=" + ny;
    path += "&pageNo=1&numOfRows=100";
    path += "&dataType=JSON";
    path += "&ServiceKey=" + SHORT_API_KEY;
    
    console.log( "path" , path);
    
   axiosInstance.get(path)
        .then(function (response) {
            resolve(response.data);
        }) .catch(function (error) { 
            reject(error)
        }) .then(function () { 
        });    
})

const _getVilageFcst = (date, time, nx, ny) => new Promise((resolve, reject) => {
    const times = ["0200", "0500", "0800", "1100", "1400", "1700", "2000", "2300"];
    
    if (!times.includes( time)) {
        reject("[ERROR] " + time + " IS NOT IN [0200, 0500, 0800, 1100, 1400, 2000, 2300]");
        return ;
    }

    var path = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst";
    path += "?base_date=" + date;
    path += "&base_time=" + time;
    path += "&nx=" + nx + "&ny=" + ny;
    path += "&pageNo=1&numOfRows=1000";
    path += "&dataType=JSON";
    path += "&ServiceKey=" + SHORT_API_KEY;
    
    console.log( "path" , path);
    
    axiosInstance.get(path)
        .then(function (response) {
            resolve(response.data);
        }) .catch(function (error) { 
            reject(error)
        }) .then(function () { 
        });    
})

const _getMidTa = (date, time, regId) => new Promise((resolve, reject) => {
    const times = ["0600", "1800"];
    
    if (!times.includes( time)) {
        reject("[ERROR] " + time + " IS NOT 6 or 18");
        return ;
    }

    const tmFc = date + "" + time;
    var path = "http://apis.data.go.kr/1360000/MidFcstInfoService/getMidTa";
    path += "?tmFc=" + tmFc;
    path += "&regId=" + regId;
    path += "&pageNo=1&numOfRows=100";
    path += "&dataType=JSON";
    path += "&ServiceKey=" + MID_API_KEY;
    
    console.log( "path" , path);
    
    axiosInstance.get(path)
        .then(function (response) {
            resolve(response.data);
        }) .catch(function (error) { 
            reject(error)
        }) .then(function () { 
        });    
});


const _getMidLandFcst = (date, time, regId) => new Promise((resolve, reject) => {
    const times = ["0600", "1800"];
    
    if (!times.includes( time)) {
        reject("[ERROR] " + time + " IS NOT 6 or 18");
        return ;
    }

    const tmFc = date + "" + time;
    var path = "http://apis.data.go.kr/1360000/MidFcstInfoService/getMidLandFcst";
    path += "?tmFc=" + tmFc;
    path += "&regId=" + regId;
    path += "&pageNo=1&numOfRows=100";
    path += "&dataType=JSON";
    path += "&ServiceKey=" + MID_API_KEY;

    console.log( "path" , path);

    axiosInstance.get(path)
        .then(function (response) {
            resolve(response.data);
        }) .catch(function (error) { 
            reject(error)
        }) .then(function () { 
        });    
});