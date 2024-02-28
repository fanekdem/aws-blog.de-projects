# Imports
# from sagemaker.rl.estimator import RLEstimator
from sagemaker.estimator import Estimator as RLEstimator
from sagemaker import image_uris

# NOTE: make sure to replace the role with an existing sagemaker execution role within your account
role = SOME_SAGEMAKER_EXECUTION_ROLE_CREATED_IN_THE_AWS_ACCOUNT

# Retrieve the required tensorflow container image
instance_type = "ml.m5.large"
image_uri = image_uris.retrieve(
    framework="tensorflow",
    region="eu-central-1",
    version="2.13",
    py_version="py310",
    image_scope="training",
    instance_type=instance_type,
)

# Metrics definition to visualize metrics within SageMaker dashboards
float_regex = "[-+]?[0-9]*[.]?[0-9]+([eE][-+]?[0-9]+)?"
metric_definitions = [
    {"Name": "episode_reward_mean", "Regex": r"episode_reward_mean\s*(%s)" % float_regex},
    {"Name": "episode_reward_max", "Regex": r"episode_reward_max\s*(%s)" % float_regex},
]

# The actual estimator
estimator = RLEstimator(
    entry_point="train-rl-cartpole-ray.py",
    source_dir="src",
    image_uri=image_uri,
    role=role,
    debugger_hook_config=False,
    instance_type=instance_type,
    instance_count=1,
    base_job_name="rl-cartpole-ray-2x",
    metric_definitions=metric_definitions,
    hyperparameters={
        # Let's override some hyperparameters
        "rl.training.config.lr": 0.0001,
    },
)

# Training start
estimator.fit(wait=False)
job_name = estimator.latest_training_job.job_name
print("Training job: %s" % job_name)
