# An unsung hero of Amazon SageMaker: Local Mode
![Image](img/sagemaker-local-cover.jpg)

The code within this folder is a companion for the blog post [An unsung hero of Amazon SageMaker: local mode](https://www.tecracer.com/blog/2024/06/an-unsung-hero-of-amazon-sagemaker-local-mode.html).

The code within this repository assumes [Python3](https://www.python.org/downloads/) and [pip](https://pip.pypa.io/en/stable/installation/) are available.


Other required dependencies are:
- [Docker Engine](https://docs.docker.com/engine/install/)
- [Docker compose plugin](https://docs.docker.com/compose/install/)
And in case you have a CUDA supported GPU and want to experiment with gpu based jobs in local mode:
-  [CUDA Toolkit](https://developer.nvidia.com/cuda-downloads)
- [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html)
Further more, 

Create a virtual environment.
```bash
$ python3 -m venv env
```

Activate the virtual environment.
```bash
$ source env/bin/activate
```

Install python dependencies.
```bash
python -m pip install "sagemaker[local]"
```

⚠️ Please note that the jobs within this folder will try to get an IAM SageMaker role named `sagemaker` and create it in case it is not available. You can avoid that by manually setting a value to the variable `role` instead of using the method `get_execution`.

Start the SageMaker training job.
```bash
python lauch_<...>.py
```



---

Title Photo by [Priscilla Du Preez 🇨🇦](https://unsplash.com/@priscilladupreez) on [Unsplash](https://unsplash.com/photos/gray-and-brown-local-sign-acNPOikiDRw)