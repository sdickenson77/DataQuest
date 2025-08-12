ReArcData Quest

This example project showcases how to download and process data from different data sources and perform some data analysis.

This repo includes a setup for a data pipeline using cdk that will deploy a lambda to aws which will run 2 python scripts: fetch_data_from_api.py  and publish_open_dataset.py
- when the fetch_data_from_api.py is processed and a new json file is downloaded it will download and run a jupyter notebook stored in s3

  Prerequisites:
  - you have an aws profile set up in ~.aws/credentials
  - you have docker, cdk, npm and nodejs installed for typescript
 

To Deploy code:
  First - must export the following environment variables (use values specific for your setup):
- S3_BUCKET_NAME=<BUCKET_NAME>
- USER_AGENT=<your_email_address>
- SQS_QUEUE_URL=<SQS_QUEUE_URL>
- CDK_DEFAULT_ACCOUNT=<ACCOUNT_ID>
- CDK_DEFAULT_REGION='<YOUR REGION>
- NOTEBOOK_S3_BUCKET=<BUCKET_NAME>
- NOTEBOOK_S3_KEY=<subfolder/ipynb filename where ipynb file exists in s3>

Once environment variables have been established run the following to install needed dependencies:  python3 -m pip install -r lambda/orchestrator/requirements.txt -t lambda/orchestrator 

next run: npx cdk synth --profile default

finally: npx cdk deploy --profile default

Once deploy is finished lmada function will be available in aws - scheduled to run once a day at 3am UTC.
