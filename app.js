const {S3, Athena} = require('aws-sdk');
const task = require('promise-util-task');
const _ = require('lodash');
const yaml = require('js-yaml');
const moment = require('moment');
const s3 = new S3();
const url = require('url');
const qs = require('qs');

let targetDate = null;
if (process.env.DATE) {
  targetDate = moment(process.env.DATE);
} else {
  targetDate = moment().subtract(1, 'day');
}

const tomorrowTargetDate = moment(targetDate).add(1, 'day');
const year = targetDate.year();
const month = targetDate.month() + 1;
const day = targetDate.date();
const t_year = tomorrowTargetDate.year();
const t_month = tomorrowTargetDate.month() + 1;
const t_day = tomorrowTargetDate.date();
let dateCriteria = null;

let config = null;
let athena = null;
let bigquery = null;
let dataset = null;
let storage = null;
let gcsBucket = null;

let schema = [];

const waitAthena = (executionId) => {
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve();
    }, 500);
  }).then(() => {
    return athena.getQueryExecution({
      QueryExecutionId: executionId
    }).promise();
  }).then((result) => {
    let state = result.QueryExecution.Status.State;

    if (state === 'RUNNING' || state === 'QUEUED') {
      return waitAthena(executionId);
    }

    if (state === 'SUCCEEDED') {
      return result;
    }

    console.dir(result);
    throw new Error(result);
  });
};

const resultAthena = (executionId, nextToken = null, results = []) => {
  let params = {QueryExecutionId: executionId, MaxResults: 1000};
  if (nextToken) {
    params.NextToken = nextToken;
  }

  return athena.getQueryResults(params).promise().then((result) => {
    results = results.concat(result.ResultSet.Rows);
    if (result.NextToken) {
      return resultAthena(executionId, result.NextToken, results);
    }

    return results;
  });
};

const executeDataQuery = (partitionValue = null, partitionBy = null) => {
  if (partitionBy && !partitionValue) {
    // partition mode but there is no partition value
    return;
  }

  const tableNamePrefix = config.gcloud.bigquery.tableNamePrefix;

  const toStr = (data) => {
    if (data === '-') {
      return null;
    }
    return data;
  };

  const toInt = (data) => {
    if (data === '-') {
      return null;
    }
    const result = parseInt(data);
    if (isNaN(result)) {
      return null;
    }
    return result;
  };

  const makeData = (ws, executionId, nextToken = null, fields = []) => {
    let params = {QueryExecutionId: executionId, MaxResults: 1000};
    if (nextToken) {
      params.NextToken = nextToken;
    }

    const fieldIndex = (fieldName) => {
      return _.findIndex(fields, (field) => {
        return field.VarCharValue === fieldName;
      });
    };

    return athena.getQueryResults(params).promise().then((result) => {
      let rows = null;
      if (nextToken === null) {
        fields = result.ResultSet.Rows[0].Data;
        rows = _.tail(result.ResultSet.Rows);
      } else {
        rows = result.ResultSet.Rows;
      }

      _.each(rows, (record) => {
        const object = {};

        let data = record.Data;

        if (data[0].VarCharValue.match(/^#/)) {
          return
        }

        for (let s in schema) {
          const sData = schema[s];
          const idx = fieldIndex(sData.name);

          if (config.log_type === 'cloudfront' && sData.name === 'time') {
            continue;
          }

          if (idx === -1) {
            continue;
          }

          switch (sData.type) {
          case 'STRING':
            object[sData.name] = toStr(data[idx].VarCharValue);
            break;
          case 'INTEGER':
            object[sData.name] = toInt(data[idx].VarCharValue);
            break;
          case 'TIMESTAMP':
            object[sData.name] = moment(data[idx].VarCharValue, 'DD/MMM/YYYY:HH:mm:ss ZZ').toDate();
            break;
          }
        }

        if (config.log_type === 'cloudfront') {
          object['time'] = moment.utc(`${data[fieldIndex('date')].VarCharValue} ${data[fieldIndex('time')].VarCharValue}`).toDate();
        }

        if (config.parser.queryToJson) {
          for (let name in config.parser.queryToJson) {
            object[name] = null;
            const conf = config.parser.queryToJson[name];
            const target = object[conf.target];
            if (!target) {
              object[name] = null;
            } else {
              if (config.log_type === 'cloudfront') {
                object[name] = JSON.stringify(qs.parse(target));
              } else {
                const sTarget = target.split(' ');
                if (sTarget.length > 1) {
                  const query = url.parse(sTarget[1]).query;
                  if (query) {
                    object[name] = JSON.stringify(qs.parse(query));
                  }
                }
              }

            }
          }
        }

        for (let name in config.parser.requestColumns) {
          object[name] = null;
          const conf = config.parser.requestColumns[name];
          const pattern = new RegExp(conf.pattern);
          const matched = object[conf.target].match(pattern);
          if (matched && matched.length > 1) {
            if (conf.type === 'Integer' || conf.type === 'Int') {
              object[name] = parseInt(matched[1]);
            } else {
              object[name] = matched[1];
            }
          }
        }

        ws.write(JSON.stringify(object) + '\n');
      });

      if (result.NextToken) {
        return makeData(ws, executionId, result.NextToken, fields);
      }

      ws.end();
    });
  };

  let query = `SELECT * FROM ${config.aws.athena.database}.${config.aws.athena.table} WHERE ${dateCriteria}`;
  if (partitionBy) {
    query += ` AND ${partitionBy} = '${partitionValue}'`;
  }
  console.log(query);

  return athena.startQueryExecution({
    QueryString: query,
    ResultConfiguration: {
      OutputLocation: `s3://${config.aws.s3.athena_result_bucket}/${config.aws.s3.athena_result_prefix}`,
      EncryptionConfiguration: {
        EncryptionOption: 'SSE_S3'
      }
    }
  }).promise().then((result) => {
    return waitAthena(result.QueryExecutionId);
  }).then((result) => {
    return new Promise((resolve, error) => {
      let fileName = null;
      if (partitionBy) {
        fileName = `${tableNamePrefix}_${targetDate.format('YYYYMMDD')}/${tableNamePrefix}_${partitionValue}_${targetDate.format('YYYYMMDD')}.json`;
      } else {
        fileName = `${tableNamePrefix}_${targetDate.format('YYYYMMDD')}.json`;
      }
      const file = gcsBucket.file(fileName);

      const ws = file.createWriteStream({resumeable: false, gzip: true});
      ws.on('error', (err) => {
        console.log(err);
        error(err);
      });
      ws.on('finish', () => {
        resolve(file);
      });
      makeData(ws, result.QueryExecution.QueryExecutionId);
    });
  }).then((file) => {
    let tableName = null;
    if (partitionBy) {
      tableName = `${tableNamePrefix}_${partitionValue}_${targetDate.format('YYYYMMDD')}`;
    } else {
      tableName = `${tableNamePrefix}_${targetDate.format('YYYYMMDD')}`;
    }
    const table = dataset.table(tableName);
    console.log('Load table', tableName);
    return table.load(file, {
      schema: {
        fields: schema
      },
      sourceFormat: 'NEWLINE_DELIMITED_JSON',
      writeDisposition: 'WRITE_TRUNCATE'
    });
  }).then((response) => {
    console.dir(response);
    console.dir(response[0].status);
    const job = bigquery.job(response[0].jobReference.jobId);

    if (response[0].status.state === 'DONE') {
      return
    }

    return new Promise(function (resolve, reject) {
      job.on('complete', function () {
        setTimeout(function () {
          resolve();
        }, 1000);
      });
      job.on('error', function (err) {
        reject(err);
      });
    });
  });
};

const executePartitionedData = (partitionBy) => {
  const partitionQuery = `SELECT DISTINCT ${partitionBy} FROM ${config.aws.athena.database}.${config.aws.athena.table} WHERE ${dateCriteria}`;
  console.log(partitionQuery);

  return athena.startQueryExecution({
    QueryString: partitionQuery,
    ResultConfiguration: {
      OutputLocation: `s3://${config.aws.s3.athena_result_bucket}/${config.aws.s3.athena_result_prefix}`,
      EncryptionConfiguration: {
        EncryptionOption: 'SSE_S3'
      }
    }
  }).promise().then((result) => {
    return waitAthena(result.QueryExecutionId);
  }).then((result) => {
    return resultAthena(result.QueryExecution.QueryExecutionId);
  }).then((result) => {
    return task.limit(_.tail(result).map((record) => {
      let serviceId = record.Data[0].VarCharValue;
      return () => {
        return executeDataQuery(serviceId, partitionBy);
      };
    }), 1);
  });
};

console.log('Get schema');
s3.getObject({Bucket: process.env.CONFIG_BUCKET, Key: process.env.CONFIG_KEY}).promise().then((obj) => {
  config = yaml.safeLoad(obj.Body.toString());
  athena = new Athena({
    apiVersion: '2017-05-18',
    region: config.aws.athena.region
  });

  dateCriteria = `((year = ${year} AND month = ${month} AND day = ${day}) OR 
(year = ${t_year} AND month = ${t_month} AND day = ${t_day}))
AND date_parse(datetime, '%d/%b/%Y:%H:%i:%s +0000') >= timestamp '${moment(targetDate).startOf('day').format('YYYY-MM-DD HH:mm:ss')}'
AND date_parse(datetime, '%d/%b/%Y:%H:%i:%s +0000') <= timestamp '${moment(targetDate).endOf('day').format('YYYY-MM-DD HH:mm:ss')}'`;

  if (config.log_type === 'cloudfront') {
    dateCriteria = `((year = ${year} AND month = ${month} AND day = ${day}) OR 
(year = ${t_year} AND month = ${t_month} AND day = ${t_day}))
AND date_parse(CONCAT(date, ' ', time), '%Y-%m-%d %H:%i:%s') >= timestamp '${moment(targetDate).startOf('day').format('YYYY-MM-DD HH:mm:ss')}'
AND date_parse(CONCAT(date, ' ', time), '%Y-%m-%d %H:%i:%s') <= timestamp '${moment(targetDate).endOf('day').format('YYYY-MM-DD HH:mm:ss')}'`;
  }

  return s3.getObject({Bucket: process.env.CONFIG_BUCKET, Key: process.env.GCLOUD_KEY}).promise();
}).then((obj) => {
  const key = JSON.parse(obj.Body.toString());
  const gcloudConfig = {
    projectId: config.gcloud.projectId,
    credentials: key
  };

  const BigQuery = require('@google-cloud/bigquery');
  bigquery = new BigQuery(gcloudConfig);
  dataset = bigquery.dataset(config.gcloud.bigquery.dataset);

  const {Storage} = require('@google-cloud/storage');
  storage = new Storage(gcloudConfig);
  gcsBucket = storage.bucket(config.gcloud.storage.bucket);

  return s3.getObject({Bucket: config.aws.s3.schema_bucket, Key: config.aws.s3.schema_object}).promise();
}).then((obj) => {
  schema = JSON.parse(obj.Body.toString());

  const msckQuery = `MSCK REPAIR TABLE ${config.aws.athena.database}.${config.aws.athena.table}`;
  console.log(msckQuery);

  return athena.startQueryExecution({
    QueryString: msckQuery,
    ResultConfiguration: {
      OutputLocation: `s3://${config.aws.s3.athena_result_bucket}/${config.aws.s3.athena_result_prefix}`,
      EncryptionConfiguration: {
        EncryptionOption: 'SSE_S3'
      }
    }
  }).promise();
}).then((result) => {
  return waitAthena(result.QueryExecutionId);
}).then(() => {
  if (config.partition) {
    return executePartitionedData(config.partition);
  } else {
    return executeDataQuery();
  }
}).then(() => {
  console.log(`Completed ${targetDate.format('YYYYMMDD')}`);
}).catch((error) => {
  console.dir(error);
});
