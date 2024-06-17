import sagemaker

sagemaker_session = sagemaker.session.Session()
instance_type = "local_gpu"
image_uri = sagemaker.image_uris.retrieve(
    framework="tensorflow",
    region=sagemaker_session.boto_region_name,
    version="2.13",
    py_version="py310",
    instance_type=instance_type,
    image_scope="training",
)
