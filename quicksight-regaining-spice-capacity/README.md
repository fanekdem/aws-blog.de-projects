# Regaining QuickSight Spice capacity

This folder contains a python script to help you find out which QuickSight datasets are using most of the SPICE capacity in your QuickSight account.


The script assumes [Python3](https://www.python.org/downloads/) is available with [pip](https://pip.pypa.io/en/stable/installation/).

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
python -m pip install boto3
```

You environment should contain proper AWS credentials for the interactions with AWS to take place.

Query your QuickSight dataset using the script the resources.
```bash
python list_top_spice_capacity_datasets.py
```

The output contains the infos about dataset for which no information could be found, as well as a list of datasets using most of your SPICE capacity.

```log
> python list_top_spice_capacity_datasets.py
Enter your AWS account ID: 123456789012
Error getting dataset size for 21ed69b7-8ffe-41d4-b6e3-d9e0b8411d13/KDS-S3-Z4891-bc8d-4e6dcf374fb3_reduced.csv: An error occurred (InvalidParameterValueException) when calling the DescribeDataSet operation: The data set type is not supported through API yet
Error getting dataset size for 4b62807e-cda3-4217-b18d-5cb1e6d3e88a/titanic-demo-file: An error occurred (InvalidParameterValueException) when calling the DescribeDataSet operation: The data set type is not supported through API yet
Error getting dataset size for 64cb01af-7def-4d23-bc63-b9e6f0dec636/cust.csv: An error occurred (InvalidParameterValueException) when calling the DescribeDataSet operation: The data set type is not supported through API yet
[{'Arn': 'arn:aws:quicksight:eu-west-1:123456789012:dataset/1ade907f-c5cc-4adc-a592-8469e5a3da9f',
  'ConsumedSpiceCapacityInBytes': 135001466281,
  'DataSetId': '1ade907f-c5cc-4adc-a592-8469e5a3da9f',
  'ImportMode': 'SPICE',
  'Name': 'global-electronics-retail'},
 {'Arn': 'arn:aws:quicksight:eu-west-1:123456789012:dataset/3acccb06-a1f3-6a5d-a7bd-e1cf963303ee',
  'ConsumedSpiceCapacityInBytes': 285941024,
  'DataSetId': '3acccb06-a1f3-6a5d-a7bd-e1cf963303ee',
  'ImportMode': 'SPICE',
  'Name': 'training02 dataset'}]
```
