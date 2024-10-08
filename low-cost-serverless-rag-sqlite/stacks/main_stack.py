import os
from pathlib import Path
from aws_cdk import (
    Stack,
    aws_lambda,
    aws_s3,
    aws_sqs,
    aws_iam,
    aws_glue,
    aws_lambda_event_sources,
    aws_s3_notifications,
)
import aws_cdk as cdk
from constructs import Construct


class RAGStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # stack_addr = self.node.addr[:8]

        output_bucket = input_bucket = aws_s3.Bucket(self, "RAG")
        input_prefix = "input"

        output_prefix = "output"
        sqlite_db_s3_key = os.path.join(output_prefix, "db.sqlite3")

        tmp_prefix = "tmp"

        sqlite_db_s3_uri = f"s3://{output_bucket.bucket_name}/{sqlite_db_s3_key}"

        lambda_import = aws_lambda.Function(
            self,
            "RAGDocumentImport",
            runtime=aws_lambda.Runtime.FROM_IMAGE,
            handler=aws_lambda.Handler.FROM_IMAGE,
            code=aws_lambda.Code.from_asset_image(
                str(Path(__file__).parent / "resources/python"),
                cmd=["lambda_import.index.lambda_handler"],
            ),
            environment={
                "OUTPUT_BUCKET": output_bucket.bucket_name,
                "OUTPUT_PREFIX": output_prefix,
                "SQLITE_DB_S3_BUCKET": output_bucket.bucket_name,
                "SQLITE_DB_S3_KEY": sqlite_db_s3_key,
            },
            memory_size=1024,
            timeout=cdk.Duration.minutes(10),
            reserved_concurrent_executions=2,
        )

        lambda_query = aws_lambda.Function(
            self,
            "RAGDocumentQuery",
            runtime=aws_lambda.Runtime.FROM_IMAGE,
            handler=aws_lambda.Handler.FROM_IMAGE,
            code=aws_lambda.Code.from_asset_image(
                str(Path(__file__).parent / "resources/python"),
                cmd=["lambda_query.index.lambda_handler"],
            ),
            environment={
                "SQLITE_DB_S3_BUCKET": output_bucket.bucket_name,
                "SQLITE_DB_S3_KEY": sqlite_db_s3_key,
            },
            memory_size=1024,
            timeout=cdk.Duration.minutes(5),
        )

        lambda_query.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "bedrock:InvokeModel",
                    "bedrock:InvokeModelWithResponseStream",
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=["*"],
            )
        )

        s3_queue = aws_sqs.Queue(self, "S3Queue", visibility_timeout=cdk.Duration.minutes(15))

        import_fifo_queue = aws_sqs.Queue(self, "LambdaQueue", fifo=True, visibility_timeout=cdk.Duration.minutes(10))

        lambda_queue_filler = aws_lambda.Function(
            self,
            "RAGImportQueueFiller",
            runtime=aws_lambda.Runtime.FROM_IMAGE,
            handler=aws_lambda.Handler.FROM_IMAGE,
            code=aws_lambda.Code.from_asset_image(
                str(Path(__file__).parent / "resources/python"),
                cmd=["lambda_queue_filler.index.lambda_handler"],
            ),
            environment={
                "SQS_QUEUE_NAME": import_fifo_queue.queue_name,
                "SQS_QUEUE_URL": import_fifo_queue.queue_url,
            },
            memory_size=1024,
            timeout=cdk.Duration.minutes(3),
        )

        import_fifo_queue.grant_send_messages(lambda_queue_filler)

        input_bucket.add_event_notification(
            aws_s3.EventType.OBJECT_CREATED,
            aws_s3_notifications.SqsDestination(s3_queue),
            aws_s3.NotificationKeyFilter(prefix=os.path.join(input_prefix, "")),
        )

        lambda_queue_filler.add_event_source(
            aws_lambda_event_sources.SqsEventSource(
                queue=s3_queue,
            )
        )

        # NOTE: Our use case makes sense as long as the import lambda function isn't concurrently invoked.
        # This can not be achieved we the queue alone, so we just reduce the Queue-Lambda concurrency to 2 here
        # and make use of lambda concurrency reservation.
        lambda_import.add_event_source(
            aws_lambda_event_sources.SqsEventSource(
                batch_size=10,
                queue=import_fifo_queue,
                max_concurrency=2,
            )
        )
        lambda_import.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "bedrock:InvokeModel",
                    "bedrock:InvokeModelWithResponseStream",
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=["*"],
            )
        )

        input_bucket.grant_read(lambda_import, os.path.join(input_prefix, "*"))
        output_bucket.grant_read_write(lambda_import, os.path.join(output_prefix, "*"))

        output_bucket.grant_read(lambda_query, os.path.join(output_prefix, "*"))
        output_bucket.grant_read_write(lambda_query, os.path.join(tmp_prefix, "*"))

        cdk.CfnOutput(self, "LambdaQueryName", value=lambda_query.function_name, description="Lambda Query name")
        cdk.CfnOutput(self, "OutputBucket", value=output_bucket.bucket_name)
        cdk.CfnOutput(self, "OutputBucketPrefix", value=output_prefix)
        cdk.CfnOutput(self, "DocumentImportFolder", value=f"s3://{input_bucket.bucket_name}/{input_prefix}/")
