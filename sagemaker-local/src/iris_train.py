from __future__ import print_function

import argparse
import joblib
import os
import pandas as pd

from sklearn import tree

MODEL_EXPORT_BASE_NAME = "model.joblib"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # Hyperparameters are described here. In this simple example we are just including one hyperparameter.
    parser.add_argument("--max_leaf_nodes", type=int, default=-1)

    # Sagemaker specific arguments. Defaults are set in the environment variables.
    parser.add_argument("--output-data-dir", type=str, default=os.environ["SM_OUTPUT_DATA_DIR"])
    parser.add_argument("--model-dir", type=str, default=os.environ["SM_MODEL_DIR"])
    parser.add_argument("--train", type=str, default=os.environ["SM_CHANNEL_TRAIN"])

    args = parser.parse_args()

    # Take the set of files and read them all into a single pandas dataframe
    input_files = [os.path.join(args.train, file) for file in os.listdir(args.train)]
    if len(input_files) == 0:
        raise ValueError(
            (
                "There are no files in {}.\n"
                + "This usually indicates that the channel ({}) was incorrectly specified,\n"
                + "the data specification in S3 was incorrectly specified or the role specified\n"
                + "does not have permission to access the data."
            ).format(args.train, "train")
        )
    raw_data = [pd.read_csv(file, header=None, engine="python") for file in input_files]
    all_data = pd.concat(raw_data).sample(frac=1)
    val_split = int(len(all_data) * 0.8)

    # labels are in the first column
    train_y = all_data.iloc[:val_split, 0]
    train_X = all_data.iloc[:val_split, 1:]
    val_y = all_data.iloc[val_split:, 0]
    val_X = all_data.iloc[val_split:, 1:]

    # Here we support a single hyperparameter, 'max_leaf_nodes'. Note that you can add as many
    # as your training may require in the ArgumentParser above.
    max_leaf_nodes = args.max_leaf_nodes

    # Now use scikit-learn's decision tree classifier to train the model.
    clf = tree.DecisionTreeClassifier(max_leaf_nodes=max_leaf_nodes)
    clf = clf.fit(train_X, train_y)

    # We save the trained model to be able to run inference later using it.
    joblib.dump(clf, os.path.join(args.model_dir, MODEL_EXPORT_BASE_NAME))

    # We print our model training and accuracy evaluation.
    print("train_accuracy: ", clf.score(train_X, train_y))
    print("validation_accuracy: ", clf.score(val_X, val_y))


def model_fn(model_dir):
    """Deserialized and return fitted model

    Note that this should have the same name as the serialized model in the main method
    """
    clf = joblib.load(os.path.join(model_dir, MODEL_EXPORT_BASE_NAME))

    return clf
