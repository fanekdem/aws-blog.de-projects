import os
from pathlib import Path
from aws_cdk import (
    Stack,
    aws_lambda,
    aws_s3,
    aws_athena,
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

        stack_addr = self.node.addr[:8]

        workgroup_name = f"rag_{stack_addr}"
        glue_database_name = f"rag_{stack_addr}"
        glue_table_name = "documents"

        output_bucket = input_bucket = aws_s3.Bucket(self, "RAG")
        input_prefix = "input"

        output_prefix = "output"
        documents_table_location_prefix = os.path.join(output_prefix, "tables", glue_table_name)

        tmp_prefix = "tmp"

        output_bucket.add_lifecycle_rule(
            prefix=os.path.join(tmp_prefix, ""),
            noncurrent_version_expiration=cdk.Duration.days(1),
            expiration=cdk.Duration.days(7),
        )

        athena_table_location = f"s3://{output_bucket.bucket_name}/{documents_table_location_prefix}"

        glue_database = aws_glue.CfnDatabase(
            self,
            "RAGDatabase",
            catalog_id=self.account,
            database_input=aws_glue.CfnDatabase.DatabaseInputProperty(
                name=glue_database_name,
            ),
        )

        columns_name_type_comment_regular = [
            ("uuid", "string", None),
            ("timestamp", "string", None),
            ("start", "bigint", None),
            ("end", "bigint", None),
            ("start_unique", "bigint", None),
            ("end_unique", "bigint", None),
            ("lsh", "string", None),
            ("document_id", "string", None),
            ("text", "string", None),
        ]
        columns_name_type_comments_partitions = [
            # ("aggregated", "string", None),
        ]

        glue_athena_table = aws_glue.CfnTable(
            self,
            "RAGTable",
            catalog_id=self.account,
            database_name=glue_database.database_input.name,
            table_input=aws_glue.CfnTable.TableInputProperty(
                description="Low cost serverless RAG Athena table",
                name=glue_table_name,
                # owner="owner",
                storage_descriptor=aws_glue.CfnTable.StorageDescriptorProperty(
                    columns=[
                        aws_glue.CfnTable.ColumnProperty(
                            name=name,
                            type=type_,
                            comment=comment,
                        )
                        for name, type_, comment in columns_name_type_comment_regular
                    ],
                    location=athena_table_location,
                    input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    serde_info=aws_glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        parameters={
                            "parquet.compression": "SNAPPY",
                            "EXTERNAL": "TRUE",
                        },
                    ),
                ),
                partition_keys=[
                    aws_glue.CfnTable.ColumnProperty(
                        name=name,
                        type=type_,
                        comment=comment,
                    )
                    for name, type_, comment in columns_name_type_comments_partitions
                ],
                table_type="EXTERNAL_TABLE",
            ),
        )
        glue_athena_table.node.add_dependency(glue_database)

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
                "DOCUMENTS_OUTPUT_PREFIX": documents_table_location_prefix,
            },
            memory_size=1024,
            timeout=cdk.Duration.minutes(5),
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
                "ATHENA_TABLE": glue_table_name,
                "ATHENA_DATABASE": glue_database.database_input.name,
                "ATHENA_WORKGROUP": workgroup_name,
            },
            memory_size=1024,
            timeout=cdk.Duration.minutes(5),
        )
        # NOTE: As we are using the AWS data wrangler we need a bit more of athena/glue permissions than normally
        lambda_query.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                    "athena:GetWorkGroup",
                    "athena:ListWorkGroups",
                    "athena:StartQueryExecution",
                    "athena:StopQueryExecution",
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:GetTable",
                    "glue:GetTables",
                    "bedrock:InvokeModel",
                    "bedrock:InvokeModelWithResponseStream",
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=["*"],
            )
        )

        aws_s3_notifications.LambdaDestination(lambda_query)
        lambda_import.add_event_source(
            aws_lambda_event_sources.S3EventSource(
                input_bucket,
                events=[aws_s3.EventType.OBJECT_CREATED],
                filters=[aws_s3.NotificationKeyFilter(prefix=os.path.join(input_prefix, ""))],
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
        output_bucket.grant_write(lambda_import, os.path.join(output_prefix, "*"))

        output_bucket.grant_read(lambda_query, os.path.join(output_prefix, "*"))
        output_bucket.grant_read_write(lambda_query, os.path.join(tmp_prefix, "*"))

        workgroup = aws_athena.CfnWorkGroup(
            self,
            "RAGAthenaWorkgroup",
            name=workgroup_name,
            description="RAG solution workgroup",
            work_group_configuration=aws_athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=aws_athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{output_bucket.bucket_name}/{tmp_prefix}/athena-results/",
                    expected_bucket_owner=self.account,
                    encryption_configuration=aws_athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_S3"
                    ),
                )
            ),
        )
        workgroup.node.add_dependency(output_bucket)

        cdk.CfnOutput(self, "LambdaQueryName", value=lambda_query.function_name, description="Lambda Query name")
        cdk.CfnOutput(self, "OutputBucket", value=output_bucket.bucket_name)
        cdk.CfnOutput(self, "OutputBucketPrefix", value=output_prefix)
        cdk.CfnOutput(self, "DocumentImportFolder", value=f"s3://{input_bucket.bucket_name}/{input_prefix}/")
        cdk.CfnOutput(self, "AthenaWorkgroupName", value=workgroup_name)
