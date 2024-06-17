# S3 prefix
prefix = "DEMO-scikit-iris"

import sagemaker
from sagemaker.local import LocalSession

# from sagemaker import get_execution_role
import boto3
import json


def get_execution_role(role_name="sagemaker", aws_account=None, aws_region=None):
    """
    Create sagemaker execution role to perform sagemaker task

    Args:
        role_name (string): name of the role to be created
        aws_account (string): aws account of the ECR repo
        aws_region (string): aws region where the repo is located
    """
    session = boto3.Session()
    aws_account = aws_account or session.client("sts").get_caller_identity()["Account"]
    aws_region = aws_region or session.region_name

    assume_role_policy_document = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": ["sagemaker.amazonaws.com", "robomaker.amazonaws.com"]},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )

    client = session.client("iam")
    try:
        client.get_role(RoleName=role_name)
    except client.exceptions.NoSuchEntityException:
        client.create_role(RoleName=role_name, AssumeRolePolicyDocument=str(assume_role_policy_document))

        print("Created new sagemaker execution role: %s" % role_name)

    client.attach_role_policy(PolicyArn="arn:aws:iam::aws:policy/AmazonSageMakerFullAccess", RoleName=role_name)

    return client.get_role(RoleName=role_name)["Role"]["Arn"]
