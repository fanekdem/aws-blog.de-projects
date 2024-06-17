import sagemaker
from sagemaker.local import LocalSession
from sagemaker.tensorflow.estimator import TensorFlow as Estimator

from helpers import get_execution_role

sagemaker_session = LocalSession()


instance_type = "local_gpu"
image_uri = sagemaker.image_uris.retrieve(
    framework="tensorflow",
    region=sagemaker_session.boto_region_name,
    version="2.13",
    py_version="py310",
    instance_type=instance_type,
    image_scope="training",
)


estimator = Estimator(
    entry_point="check_gpu.py",
    source_dir="src",
    instance_type="local_gpu",
    instance_count=1,
    role=get_execution_role(),
    image_uri=image_uri,
)

estimator.fit()
