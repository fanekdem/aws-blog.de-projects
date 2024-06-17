from sagemaker.local import LocalSession
from sagemaker.sklearn.estimator import SKLearn as Estimator

from helpers import get_execution_role

sagemaker_session = LocalSession()
region = sagemaker_session.boto_region_name


s3_data_prefix = "demo-scikit-iris"
train_input = sagemaker_session.upload_data("./data/iris.csv", key_prefix=s3_data_prefix)


estimator = Estimator(
    entry_point="iris_train.py",
    source_dir="src",
    framework_version="1.2-1",
    instance_type="local",
    instance_count=1,
    role=get_execution_role(),
    hyperparameters={"max_leaf_nodes": 30},
    verbose=0,
)

estimator.fit({"train": train_input})
