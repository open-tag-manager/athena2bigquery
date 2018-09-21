# S3WEBlog Athena to BigQuery

This product provides way to import s3weblog data to BigQuery through AWS Athena.

Copyright

see ./LICENSE


## Setup

- Use s3weblog_to_athena (https://github.com/open-tag-manager/s3weblog2athena) to convert data
for AWS Athena, and make dataset and table.
- Prepare dataset in BigQuery.


### Configuration

- Donwload Google Cloud access key by JSON format..
- Copy `config/sample.yml` to `config/${env}.yml`.
- Upload JSON key and yml file to preffer S3 Bucket. (Please encrypt data with KMS or SSE)

### AWS Batch Configuration


- build docker image

```
docker build -t s3weblog_athena_to_bigquery:latest .
```

- Make ECS Repository and push docker image.

```
AWS_PROFILE=xxx aws ecr get-login --no-include-email --region REGION
# copy login command and execute it
docker tag s3weblog_athena_to_bigquery:latest xxxxxx.dkr.ecr.REGION.amazonaws.com/s3weblog_athena_to_bigquery:latest
docker push xxxxxx.dkr.ecr.REGION.amazonaws.com/s3weblog_athena_to_bigquery:latest
```

- Make job definition to AWS Batch
    - Make sure Job Role that allows Athena and S3 access. (See: http://docs.aws.amazon.com/athena/latest/ug/access.html)
    - Container image: xxxxxx.dkr.ecr.REGION.amazonaws.com/s3weblog_athena_to_bigquery:latest
    - Set following enviroment variable to Job Definition
        - `CONFIG_BUCKET`: configuration bucket
        - `CONFIG_KEY`: configuration key file (YAML) file ObjectKey
        - `GCLOUD_KEY`: gcloud key file (JSON) file ObjectKey
- Execute job without enviroment parameters
    - As a default, a job retrieve yesterday's data. If you want to set specific day, use **DATE** environment variable. (DATE=YYYYMMDD)
- Set trigger lambda function to execute 1time/day
    - Copy `trigger/config/sample.yml` to `trigger/config/dev.yml` or `trigger/config/dev.yml`. And update variable for each enviroments.
    - Deploy and test it.

```
npm install -g serverless
npm install -g yarn
cd trigger
yarn install
AWS_PROFILE=xxx sls deploy --stage=(dev|prod)
```

## Local execution (For test)

```
docker build -t s3weblog_athena_to_bigquery:latest .
docker run -e "AWS_ACCESS_KEY_ID=XXX" \
           -e "AWS_SECRET_ACCESS_KEY=xxx" \
           -e "CONFIG_BUCKET=xxx" \
           -e "CONFIG_KEY=xxx" \
           -e "GCLOUD_KEY=xxxx" \
           -e "DATE=YYYYMMDD" \
           -it s3weblog_athena_to_bigquery:latest
```
