import pathlib
from sagemaker.local import LocalSession
from sagemaker.sklearn.estimator import SKLearn as Estimator
from sagemaker.workflow.pipeline_context import LocalPipelineSession

from helpers import get_execution_role

sagemaker_session = pipeline_session = LocalPipelineSession()

train_input = "file://" + str(pathlib.Path(__file__).parent.joinpath("data"))

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
