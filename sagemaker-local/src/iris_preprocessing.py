from sklearn.model_selection import train_test_split
import argparse
import os
import pandas as pd

PROCESSING_DIR = "/opt/ml/processing/"
PROCESSING_INPUT_DATA_DIR = os.path.join(PROCESSING_DIR, "input", "data")

if __name__ == "__main__":
    input_data_dir = PROCESSING_INPUT_DATA_DIR

    # Take the set of files and read them all into a single pandas DataFrame
    input_files = [os.path.join(input_data_dir, file) for file in os.listdir(input_data_dir) if file.endswith(".csv")]
    if len(input_files) == 0:
        raise ValueError(
            (
                "There are no files in {}.\n"
                + "This usually indicates that the channel ({}) was incorrectly specified,\n"
                + "the data specification in S3 was incorrectly specified or the role specified\n"
                + "does not have permission to access the data."
            ).format(input_data_dir, "data")
        )

    raw_data = [pd.read_csv(file, header=None, engine="python") for file in input_files]

    # We would normally apply preprocessing here, but it is not necessary for this example
    clean_data = pd.concat(raw_data)

    train_df, test_df = train_test_split(clean_data, test_size=0.2, shuffle=True)

    train_df.to_csv(os.path.join(PROCESSING_DIR, "train", "train.csv"), header=False, index=False)
    test_df.to_csv(os.path.join(PROCESSING_DIR, "test", "test.csv"), header=False, index=False)
