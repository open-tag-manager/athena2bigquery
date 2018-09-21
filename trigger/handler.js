'use strict';

const {Batch} = require('aws-sdk');
const moment = require('moment');

module.exports.trigger = (event, context, callback) => {
  const batch = new Batch();
  console.log('Make queues');
  console.log(event);

  let targetDate = moment();
  if (event.time) {
    targetDate = moment(event.time);
  }
  targetDate = targetDate.subtract(1, 'day');

  const parameters = {
    jobName: `${process.env.JOB_NAME_BASE}_${targetDate.format('YYYYMMDD')}`,
    jobDefinition: process.env.JOB_DEFINITION,
    jobQueue: process.env.JOB_QUEUE,
    containerOverrides: {
      environment: [
        {name: 'DATE', value: targetDate.format('YYYYMMDD')}
      ]
    }
  };

  batch.submitJob(parameters).promise().then(() => {
    callback();
  });
};

