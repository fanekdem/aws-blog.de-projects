from typing import TypedDict, Literal
import boto3

ImportMode = Literal["SPICE", "DIRECT_QUERY"]


class DatasetSummary(TypedDict):
    Arn: str
    DataSetId: str
    Name: str
    CreatedTime: str
    LastUpdatedTime: str
    ImportMode: ImportMode
    RowLevelPermissionTagConfigurationApplied: bool
    ColumnLevelPermissionRulesApplied: bool


class DatasetSizeInfo(TypedDict):
    Arn: str
    DataSetId: str
    Name: str
    ImportMode: ImportMode
    ConsumedSpiceCapacityInBytes: int


def list_quicksight_datasets_summaries(account_id) -> list[DatasetSummary]:
    """
    Lists all QuickSight dataset ARNs for a given AWS account ID.

    Args:
        account_id (str): The AWS account ID.

    Returns:
        list: A list of dataset ARNs.
    """
    quicksight = boto3.client("quicksight")
    response = quicksight.list_data_sets(AwsAccountId=account_id)

    return response["DataSetSummaries"]


def describe_quicksight_dataset(account_id, dataset_id) -> dict:
    """
    Describes a QuickSight dataset for a given AWS account ID and dataset ID.

    Args:
        account_id (str): The AWS account ID.
        dataset_id (str): The ID of the dataset.

    Returns:
        dict: The dataset information.
    """
    quicksight = boto3.client("quicksight")
    response = quicksight.describe_data_set(AwsAccountId=account_id, DataSetId=dataset_id)

    return response["DataSet"]


def get_datasets_size_infos(datasets_summaries: list[DatasetSummary], account_id: str) -> list[DatasetSizeInfo]:
    """
    Returns a list of dataset size information objects for the given datasets.

    Args:
        datasets_summaries (list): A list of dataset summary objects.
        account_id (str): The AWS account ID.

    Returns:
        list: A list of dataset size information objects.
    """
    results = []
    for dataset_summary in datasets_summaries:
        dataset_size_info = DatasetSizeInfo(
            {
                "Arn": dataset_summary["Arn"],
                "DataSetId": dataset_summary["DataSetId"],
                "Name": dataset_summary["Name"],
                "ImportMode": dataset_summary["ImportMode"],
                "ConsumedSpiceCapacityInBytes": 0,
            }
        )

        if dataset_summary["ImportMode"] == "SPICE":
            try:
                dataset = describe_quicksight_dataset(account_id=account_id, dataset_id=dataset_summary["DataSetId"])
                if dataset:
                    dataset_size_info["ConsumedSpiceCapacityInBytes"] = dataset["ConsumedSpiceCapacityInBytes"]
            except Exception as e:
                print(f"Error getting dataset size for {dataset_summary['DataSetId']}/{dataset_summary['Name']}: {e}")

        results.append(dataset_size_info)
    return results


def get_top_spice_capacity_datasets(
    datasets_size_infos: list[DatasetSizeInfo], top_n: int = 10, minimum_capacity_in_bytes: int = 128 * 1024 * 1024
) -> list[DatasetSizeInfo]:
    """
    Returns the top N datasets with the highest consumed Spice capacity, filtered by a minimum capacity.

    Args:
        datasets_size_infos (list): A list of dataset size information objects.
        top_n (int): The number of top datasets to return.
        minimum_capacity_in_bytes (int): The minimum capacity in bytes to filter datasets.

    Returns:
        list: A list of dataset size information objects for the top N datasets.
    """
    filtered_datasets_size_infos = [
        d for d in datasets_size_infos if d["ConsumedSpiceCapacityInBytes"] >= minimum_capacity_in_bytes
    ]
    sorted_datasets = sorted(
        filtered_datasets_size_infos, key=lambda x: x["ConsumedSpiceCapacityInBytes"], reverse=True
    )
    return sorted_datasets[:top_n]


def delete_quicksight_dataset(account_id: str, dataset_or_id: str | DatasetSizeInfo | DatasetSummary) -> None:
    """
    Deletes a QuickSight dataset for a given AWS account ID and dataset ID.

    Args:
        account_id (str): The AWS account ID.
        dataset_id (str): The ID of the dataset.

    Returns:
        None
    """
    quicksight = boto3.client("quicksight")

    if isinstance(dataset_or_id, DatasetSizeInfo):
        dataset_id = dataset_or_id["DataSetId"]
    elif isinstance(dataset_or_id, DatasetSummary):
        dataset_id = dataset_or_id["DataSetId"]
    else:
        dataset_id = dataset_or_id
    return quicksight.delete_data_set(AwsAccountId=account_id, DataSetId=dataset_id)


def list_top_spice_capacity_datasets(
    account_id: str, top_n: int = 10, minimum_capacity_in_bytes: int = 128 * 1024 * 1024
) -> list[DatasetSizeInfo]:
    """
    Lists the top N QuickSight datasets with the highest consumed Spice capacity.
    Only datasets with at least the configured minimum capacity are returned.

    Args:
        account_id (str): The AWS account ID.
        top_n (int): The number of top datasets to return.
        minimum_capacity_in_bytes (int): The minimum capacity in bytes to filter datasets.

    Returns:
        list: A list of dataset size information objects for the top N datasets.
    """
    datasets_summaries = list_quicksight_datasets_summaries(account_id=account_id)
    dataset_size_infos = get_datasets_size_infos(datasets_summaries, account_id=account_id)
    return get_top_spice_capacity_datasets(
        dataset_size_infos, top_n=top_n, minimum_capacity_in_bytes=minimum_capacity_in_bytes
    )


if __name__ == "__main__":
    from pprint import pprint

    account_id = input("Enter your AWS account ID: ")
    pprint(list_top_spice_capacity_datasets(account_id=account_id))
