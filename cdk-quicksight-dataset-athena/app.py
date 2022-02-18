#!/usr/bin/env python3
import os

import aws_cdk as cdk
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_iam as iam
import aws_cdk.aws_athena as athena
import aws_cdk.aws_quicksight as quicksight
from constructs import Construct


APPLICATION = "cdk-quicksight-dataset-athena"

TAGS = {
    "application": APPLICATION,
}

BUCKET_NAME = os.getenv("BUCKET_NAME")
QUICKSIGHT_USERNAME = os.getenv("QUICKSIGHT_USERNAME")

app = cdk.App()


class QSAthenaTitanicStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        bucket_name: str,
        qs_username: str,
        athena_database_name: str,
        tags: dict = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # AWS defined service role name
        qs_service_role_names = [
            "aws-quicksight-service-role-v0",
            "aws-quicksight-s3-consumers-role-v0",
        ]

        athena_output_prefix = "athena-results"
        qs_managed_policy = iam.CfnManagedPolicy(
            self,
            "QuickSightPolicy",
            managed_policy_name="QuickSightDemoAthenaS3Policy",
            policy_document=dict(
                Statement=[
                    dict(
                        Action=["s3:ListAllMyBuckets"],
                        Effect="Allow",
                        Resource=["arn:aws:s3:::*"],
                    ),
                    dict(
                        Action=["s3:ListBucket"],
                        Effect="Allow",
                        Resource=[
                            f"arn:aws:s3:::{bucket_name}",
                        ],
                    ),
                    dict(
                        Action=[
                            "s3:GetObject",
                            "s3:List*",
                        ],
                        Effect="Allow",
                        Resource=[
                            f"arn:aws:s3:::{bucket_name}/tables/*",
                        ],
                    ),
                    dict(
                        Action=[
                            "s3:GetObject",
                            "s3:List*",
                            "s3:AbortMultipartUpload",
                            "s3:PutObject",
                        ],
                        Effect="Allow",
                        Resource=[
                            f"arn:aws:s3:::{bucket_name}/{athena_output_prefix}/*",
                        ],
                    ),
                ],
                Version="2012-10-17",
            ),
            roles=qs_service_role_names,
        )

        qs_principal_arn = f"arn:aws:quicksight:{self.region}:{self.account}:user/default/{qs_username}"

        qs_data_source_permissions = [
            quicksight.CfnDataSource.ResourcePermissionProperty(
                principal=qs_principal_arn,
                actions=[
                    "quicksight:DescribeDataSource",
                    "quicksight:DescribeDataSourcePermissions",
                    "quicksight:PassDataSource",
                ],
            ),
        ]

        qs_dataset_permissions = [
            quicksight.CfnDataSet.ResourcePermissionProperty(
                principal=qs_principal_arn,
                actions=[
                    "quicksight:DescribeDataSet",
                    "quicksight:DescribeDataSetPermissions",
                    "quicksight:PassDataSet",
                    "quicksight:DescribeIngestion",
                    "quicksight:ListIngestions",
                ],
            )
        ]

        athena_workgroup_name = f"athena-titanic-wg"
        athena_workgroup = athena.CfnWorkGroup(
            self,
            "Workgroup",
            name=athena_workgroup_name,
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{bucket_name}/{athena_output_prefix}/",
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_S3"
                    ),
                )
            ),
            recursive_delete_option=True,
        )

        qs_principal_arn = f"arn:aws:quicksight:{self.region}:{self.account}:user/default/{qs_username}"

        qs_athena_data_source_name = "athena-titanic"
        qs_athena_data_source = quicksight.CfnDataSource(
            self,
            "AthenaDataSource",
            name=qs_athena_data_source_name,
            data_source_parameters=quicksight.CfnDataSource.DataSourceParametersProperty(
                athena_parameters=quicksight.CfnDataSource.AthenaParametersProperty(
                    work_group=athena_workgroup_name
                )
            ),
            type="ATHENA",
            aws_account_id=self.account,
            data_source_id=qs_athena_data_source_name,
            ssl_properties=quicksight.CfnDataSource.SslPropertiesProperty(
                disable_ssl=False
            ),
            permissions=qs_data_source_permissions,
        )

        qs_athena_data_source.add_depends_on(qs_managed_policy)

        qs_athena_dataset_titanic_physical_table = (
            quicksight.CfnDataSet.PhysicalTableProperty(
                relational_table=quicksight.CfnDataSet.RelationalTableProperty(
                    data_source_arn=qs_athena_data_source.attr_arn,
                    input_columns=[
                        quicksight.CfnDataSet.InputColumnProperty(
                            name="Survived", type="INTEGER"
                        ),
                        quicksight.CfnDataSet.InputColumnProperty(
                            name="Pclass", type="INTEGER"
                        ),
                        quicksight.CfnDataSet.InputColumnProperty(
                            name="Name", type="STRING"
                        ),
                        quicksight.CfnDataSet.InputColumnProperty(
                            name="Sex", type="STRING"
                        ),
                        quicksight.CfnDataSet.InputColumnProperty(
                            name="Age", type="DECIMAL"
                        ),
                        quicksight.CfnDataSet.InputColumnProperty(
                            name="Siblings/Spouses Aboard", type="INTEGER"
                        ),
                        quicksight.CfnDataSet.InputColumnProperty(
                            name="Parents/Children Aboard", type="INTEGER"
                        ),
                        quicksight.CfnDataSet.InputColumnProperty(
                            name="Fare", type="DECIMAL"
                        ),
                    ],
                    catalog="AWSDataCatalog",
                    schema=athena_database_name,
                    name="titanic",
                )
            )
        )

        qs_import_mode = "SPICE"
        qs_dataset_titanic_name = "athena-titanic-ds"
        qs_athena_dataset_titanic = quicksight.CfnDataSet(
            self,
            f"Dataset-athena-titanic",
            import_mode=qs_import_mode,
            name=qs_dataset_titanic_name,
            aws_account_id=self.account,
            data_set_id=qs_dataset_titanic_name,
            physical_table_map={
                "athena-titanic-table": qs_athena_dataset_titanic_physical_table
            },
            permissions=qs_dataset_permissions,
        )

        sql_statement = f"""
            SELECT
                Survived,
                Name,
                Sex,
                "Siblings/Spouses Aboard"+"Parents/Children Aboard" AS Related
            FROM {athena_database_name}.titanic
        """
        qs_athena_dataset_titanic_physical_table_sql = (
            quicksight.CfnDataSet.PhysicalTableProperty(
                custom_sql=quicksight.CfnDataSet.CustomSqlProperty(
                    name="titanic-sql",
                    data_source_arn=qs_athena_data_source.attr_arn,
                    sql_query=sql_statement,
                    columns=[
                        quicksight.CfnDataSet.InputColumnProperty(
                            name="Survived", type="INTEGER"
                        ),
                        quicksight.CfnDataSet.InputColumnProperty(
                            name="Name", type="STRING"
                        ),
                        quicksight.CfnDataSet.InputColumnProperty(
                            name="Sex", type="STRING"
                        ),
                        quicksight.CfnDataSet.InputColumnProperty(
                            name="Related", type="INTEGER"
                        ),
                    ],
                ),
            )
        )

        qs_dataset_titanic_sql_name = "athena-titanic-sql-ds"
        qs_athena_dataset_titanic_sql = quicksight.CfnDataSet(
            self,
            f"Dataset-athena-titanic-sql",
            import_mode=qs_import_mode,
            name=qs_dataset_titanic_sql_name,
            aws_account_id=self.account,
            data_set_id=qs_dataset_titanic_sql_name,
            physical_table_map={
                "athena-titanic-table-sql": qs_athena_dataset_titanic_physical_table_sql
            },
            permissions=qs_dataset_permissions,
        )

        ### Set tags
        if tags:
            for key, value in tags.items():
                self.tags.set_tag(key, value)


qs_athena_stack = QSAthenaTitanicStack(
    scope=app,
    construct_id="athenatitanic",
    bucket_name=BUCKET_NAME,
    qs_username=QUICKSIGHT_USERNAME,
    athena_database_name="default",
    stack_name=f"{APPLICATION}-athenatitanic",
)

app.synth()
