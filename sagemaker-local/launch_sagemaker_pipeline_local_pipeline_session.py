from sagemaker.workflow.pipeline_context import LocalPipelineSession
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.sklearn.estimator import SKLearn
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.processing import ProcessingInput, ProcessingOutput
from sagemaker.workflow.steps import ProcessingStep, TrainingStep
from helpers import get_execution_role


local_pipeline_session = LocalPipelineSession()

region = local_pipeline_session.boto_region_name
role = get_execution_role()
default_bucket = local_pipeline_session.default_bucket()

s3_data_prefix = "demo-scikit-iris"
input_data = local_pipeline_session.upload_data("./data/iris.csv", key_prefix=s3_data_prefix)

framework_version = "1.2-1"


processor = SKLearnProcessor(
    framework_version=framework_version,
    instance_type="ml.m5.xlarge",
    instance_count=1,
    base_job_name="sklearn-abalone-process",
    role=role,
)


step_process = ProcessingStep(
    name="IrisPreprocess",
    inputs=[
        ProcessingInput(source=input_data, destination="/opt/ml/processing/input/data"),
    ],
    outputs=[
        ProcessingOutput(output_name="train", source="/opt/ml/processing/train"),
        ProcessingOutput(output_name="test", source="/opt/ml/processing/test"),
    ],
    processor=processor,
    code="src/iris_preprocessing.py",
)

estimator = SKLearn(
    entry_point="iris_train.py",
    source_dir="src",
    framework_version="1.2-1",
    instance_type="ml.m5.xlarge",
    instance_count=1,
    role=get_execution_role(),
    hyperparameters={"max_leaf_nodes": 30},
)

step_train = TrainingStep(
    depends_on=[step_process],
    name="TrainModel",
    estimator=estimator,
    inputs={"train": step_process.properties.ProcessingOutputConfig.Outputs["train"].S3Output.S3Uri},
)


pipeline = Pipeline(
    "local-pipeline",
    sagemaker_session=local_pipeline_session,
    steps=[step_process, step_train],
)
pipeline.upsert(role_arn=role)
pipeline.start()
