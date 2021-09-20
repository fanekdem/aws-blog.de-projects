#!/usr/bin/env python3
import os

import aws_cdk as cdk
import aws_cdk.aws_iam as iam
import aws_cdk.aws_quicksight as quicksight
from constructs import Construct

APPLICATION = "cdk-quicksight-dataset-s3"

TAGS = {
    "application": APPLICATION,
}

BUCKET_NAME = os.getenv("BUCKET_NAME")
QUICKSIGHT_USERNAME = os.getenv("QUICKSIGHT_USERNAME")

app = cdk.App()


class QSS3TitanicStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        bucket_name: str,
        manifest_key: str,
        qs_username: str,
        tags: dict = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # AWS defined service role name
        qs_service_role_names = [
            "aws-quicksight-service-role-v0",
            "aws-quicksight-s3-consumers-role-v0",
        ]

        # Attach the application policies to the service role(s)
        qs_managed_policy = iam.CfnManagedPolicy(
            self,
            "QuickSightPolicy",
            managed_policy_name="QuickSightDemoS3Policy",
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
                            f"arn:aws:s3:::{bucket_name}/files/*",
                        ],
                    ),
                ],
                Version="2012-10-17",
            ),
            roles=qs_service_role_names,
        )

        # Quicksight permissions
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

        # Quicksight data source
        qs_s3_data_source_name = "s3-titanic"
        qs_s3_data_source = quicksight.CfnDataSource(
            scope=self,
            id="S3Datasource",
            name=qs_s3_data_source_name,
            data_source_parameters=quicksight.CfnDataSource.DataSourceParametersProperty(
                s3_parameters=quicksight.CfnDataSource.S3ParametersProperty(
                    manifest_file_location=quicksight.CfnDataSource.ManifestFileLocationProperty(
                        bucket=bucket_name,
                        key=manifest_key,
                    )
                )
            ),
            type="S3",
            aws_account_id=self.account,
            data_source_id=qs_s3_data_source_name,
            ssl_properties=quicksight.CfnDataSource.SslPropertiesProperty(
                disable_ssl=False
            ),
            permissions=qs_data_source_permissions,
        )

        qs_s3_data_source.add_depends_on(qs_managed_policy)

        # Quicksight physical table
        qs_s3_dataset_titanic_physical_table = (
            quicksight.CfnDataSet.PhysicalTableProperty(
                s3_source=quicksight.CfnDataSet.S3SourceProperty(
                    data_source_arn=qs_s3_data_source.attr_arn,
                    upload_settings=quicksight.CfnDataSet.UploadSettingsProperty(
                        contains_header=True,
                        delimiter=",",
                        format="CSV",
                    ),
                    input_columns=[
                        quicksight.CfnDataSet.InputColumnProperty(
                            name="Survived", type="STRING"
                        ),
                        quicksight.CfnDataSet.InputColumnProperty(
                            name="Pclass", type="STRING"
                        ),
                        quicksight.CfnDataSet.InputColumnProperty(
                            name="Name", type="STRING"
                        ),
                        quicksight.CfnDataSet.InputColumnProperty(
                            name="Sex", type="STRING"
                        ),
                        quicksight.CfnDataSet.InputColumnProperty(
                            name="Age", type="STRING"
                        ),
                        quicksight.CfnDataSet.InputColumnProperty(
                            name="Siblings/Spouses Aboard", type="STRING"
                        ),
                        quicksight.CfnDataSet.InputColumnProperty(
                            name="Parents/Children Aboard", type="STRING"
                        ),
                        quicksight.CfnDataSet.InputColumnProperty(
                            name="Fare", type="STRING"
                        ),
                    ],
                )
            )
        )

        # Quicksight logical table
        qs_s3_dataset_titanic_logical_table = quicksight.CfnDataSet.LogicalTableProperty(
            alias="s3-titanic-cast",
            source=quicksight.CfnDataSet.LogicalTableSourceProperty(
                physical_table_id="s3-titanic"
            ),
            data_transforms=[
                quicksight.CfnDataSet.TransformOperationProperty(
                    cast_column_type_operation=quicksight.CfnDataSet.CastColumnTypeOperationProperty(
                        column_name="Survived", new_column_type="INTEGER"
                    )
                ),
                quicksight.CfnDataSet.TransformOperationProperty(
                    cast_column_type_operation=quicksight.CfnDataSet.CastColumnTypeOperationProperty(
                        column_name="Pclass", new_column_type="INTEGER"
                    )
                ),
                quicksight.CfnDataSet.TransformOperationProperty(
                    cast_column_type_operation=quicksight.CfnDataSet.CastColumnTypeOperationProperty(
                        column_name="Age", new_column_type="INTEGER"
                    )
                ),
                quicksight.CfnDataSet.TransformOperationProperty(
                    cast_column_type_operation=quicksight.CfnDataSet.CastColumnTypeOperationProperty(
                        column_name="Siblings/Spouses Aboard",
                        new_column_type="INTEGER",
                    )
                ),
                quicksight.CfnDataSet.TransformOperationProperty(
                    cast_column_type_operation=quicksight.CfnDataSet.CastColumnTypeOperationProperty(
                        column_name="Parents/Children Aboard",
                        new_column_type="INTEGER",
                    )
                ),
                quicksight.CfnDataSet.TransformOperationProperty(
                    cast_column_type_operation=quicksight.CfnDataSet.CastColumnTypeOperationProperty(
                        column_name="Fare", new_column_type="DECIMAL"
                    )
                ),
            ],
        )

        # Quicksight dataset
        qs_import_mode = "SPICE"
        qs_s3_dataset_titanic_name = "s3-titanic-ds"
        qs_s3_dataset_titanic = quicksight.CfnDataSet(
            scope=self,
            id="S3Titanic",
            aws_account_id=self.account,
            physical_table_map={"s3-titanic": qs_s3_dataset_titanic_physical_table},
            logical_table_map={"s3-titanic": qs_s3_dataset_titanic_logical_table},
            name=qs_s3_dataset_titanic_name,
            data_set_id=qs_s3_dataset_titanic_name,
            permissions=qs_dataset_permissions,
            import_mode=qs_import_mode,
        )

        # Resources tagging
        if tags:
            for key, value in tags.items():
                self.tags.set_tag(key, value)


qs_s3_stack = QSS3TitanicStack(
    scope=app,
    construct_id="s3titanic",
    bucket_name=BUCKET_NAME,
    manifest_key="files/manifest.json",
    qs_username=QUICKSIGHT_USERNAME,
    stack_name=f"{APPLICATION}-s3titanic",
    tags=TAGS,
)

app.synth()
