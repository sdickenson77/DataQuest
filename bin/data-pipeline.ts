import 'source-map-support/register';
import { App } from 'aws-cdk-lib';
import { DataPipelineStack } from '../lib/data-pipeline-stack';

const app = new App();

 // CDK resolves account/region from AWS profile
new DataPipelineStack(app, 'DataPipelineStack');
