import 'source-map-support/register';
import { App } from 'aws-cdk-lib';
import { DataPipelineStack } from '../lib/data-pipeline-stack';

const app = new App();

new DataPipelineStack(app, 'DataPipelineStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
});
