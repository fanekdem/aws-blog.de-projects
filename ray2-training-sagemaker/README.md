# Reinforcement learning with Ray 2.x on SageMaker
![Image](img/rl-ray2-sagemaker-cover.jpg)

The code within this folder is a companion for the blog post [Reinforcement learning with Ray 2.x on SageMaker](https://www.tecracer.com/blog/2024-03-01-reinforcement-learning-with-ray-2.x-on-sagemaker.html).

The project assumes [Python3](https://www.python.org/downloads/) is available with [pip](https://pip.pypa.io/en/stable/installation/).

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
python -m pip install "sagemaker>=sagemaker==2.208.0"
```

Update the variable `role` in the script `start_sagemaker_training.py` so that it contains a valid SageMaker execution role from your AWS account.
```python
role = SOME_SAGEMAKER_EXECUTION_ROLE_CREATED_IN_THE_AWS_ACCOUNT
```

Start the SageMaker training job.
```bash
python start_sagemaker_training.py
```



---

Title Photo by [K. Mitch Hodge](https://unsplash.com/@kmitchhodge) on [Unsplash](https://unsplash.com/photos/black-traffic-light-5XrFWyYdHBM)