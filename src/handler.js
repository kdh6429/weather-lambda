
const commonUtil = require('./module/common_util');

module.exports.helloWorld = (event, context, callback) => {
  const response = {
    statusCode: 200,
    headers: {
      'Access-Control-Allow-Origin': '*', // Required for CORS support to work
    },
    body: JSON.stringify({
      message: 'Go Serverless v1.0! Your function executed successfully!',
      input: event,
    }),
  };

  commonUtil.sendtoSlack(response).then( () => {
    console.log("[console.log] response '" + response);
    callback(null, response);
  });
};
