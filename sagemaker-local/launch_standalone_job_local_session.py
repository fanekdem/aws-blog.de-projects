from sagemaker.local import LocalSession
from sagemaker.sklearn.estimator import SKLearn as Estimator

from helpers import get_execution_role

sagemaker_session = LocalSession()
s3_data_prefix = "demo-scikit-iris"
train_input = sagemaker_session.upload_data("./data/iris.csv", key_prefix=s3_data_prefix)

estimator = Estimator(
    entry_point="iris_train.py",
    source_dir="src",
    framework_version="1.2-1",
    instance_type="ml.m5.xlarge",
    role=get_execution_role(),
    sagemaker_session=LocalSession(),
    hyperparameters={"max_leaf_nodes": 30},
)

estimator.fit({"train": train_input})
