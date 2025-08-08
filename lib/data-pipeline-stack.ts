import { Stack, StackProps, Duration, RemovalPolicy } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as logs from 'aws-cdk-lib/aws-logs';

export class DataPipelineStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const dataBucket = s3.Bucket.fromBucketName(this, 'DataBucket', 'rearc-part1');


    const orchestratorLogGroup = new logs.LogGroup(this, 'PipelineOrchestratorLogGroup', {
      retention: logs.RetentionDays.ONE_MONTH,
    });

    const orchestratorFn = new lambda.Function(this, 'PipelineOrchestratorFn', {
      runtime: lambda.Runtime.PYTHON_3_11,
      memorySize: 512,
      timeout: Duration.minutes(3),
      handler: 'orchestrator_lambda.lambda_handler', // <file>.<function>
      code: lambda.Code.fromAsset('lambda/orchestrator'),
      environment: {
        S3_BUCKET_NAME: dataBucket.bucketName,
      },
      logGroup: orchestratorLogGroup,
    });

    dataBucket.grantReadWrite(orchestratorFn);

    // Optional: run daily at 03:00 UTC
    new events.Rule(this, 'PipelineDailySchedule', {
      schedule: events.Schedule.cron({ minute: '0', hour: '3' }),
      targets: [new targets.LambdaFunction(orchestratorFn)],
    });


  }
}