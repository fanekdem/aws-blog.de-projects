import glob
import os
import shutil

import gymnasium as gym
import ray
from ray.train import CheckpointConfig
from ray.tune import run_experiments
from ray.tune.registry import register_env

ENV_NAME = "CartPole-v1"
INTERMEDIATE_DIR = "/opt/ml/output/intermediate"
FINAL_MODEL_DIR = os.getenv("SM_MODEL_DIR", "/opt/ml/model")
METADATA_DIR = "/opt/ml/output/data"


def create_environment(env_config):
    return gym.make(ENV_NAME)


class SageMakerRayLauncher(object):
    def __init__(self) -> None:
        self.num_cpus = int(os.environ.get("SM_NUM_CPUS", 1))
        self.num_gpus = int(os.environ.get("SM_NUM_GPUS", 0))
        self.ray_config = {"num_cpus": self.num_cpus, "num_gpus": self.num_gpus}

    def register_env_creator(self):
        """Register the (custom) env to make it available to the ray nodes"""
        register_env(ENV_NAME, create_environment)

    def get_experiment_config(self):
        experiment_config = {
            "training": {
                "env": ENV_NAME,
                "run": "PPO",
                "stop": {"training_iteration": 300, "episode_reward_mean": 500},
                "config": {
                    "framework": "tf2",
                    "num_sgd_iter": 30,
                    "lr": 0.0001,
                    "sgd_minibatch_size": 128,
                    "train_batch_size": 4000,
                    "model": {"free_log_std": True},
                    "num_workers": (self.num_cpus - 1),
                    "num_gpus": self.num_gpus,
                    "batch_mode": "truncate_episodes",
                },
                "storage_path": INTERMEDIATE_DIR,
                "checkpoint_config": CheckpointConfig(checkpoint_at_end=True),
            }
        }

        return experiment_config

    def save_experiment_metadata(self, trial_path):
        extensions_to_select = ["*.csv", "*.pkl", "*.json"]

        files_to_copy: list[str] = []
        for extension in extensions_to_select:
            files_to_copy.extend(glob.glob(os.path.join(trial_path, extension)))

        for fpath in files_to_copy:
            if fpath.endswith("params.pkl") or fpath.endswith("params.json"):
                shutil.copy(fpath, FINAL_MODEL_DIR)
            else:
                shutil.copy(fpath, METADATA_DIR)

    def launch(self):
        """Actual entry point into the class instance where everything happens.
        Lots of delegating to classes that are in subclass or can be over-ridden.
        """
        self.register_env_creator()

        ray.init(**self.ray_config)

        experiment_config = self.get_experiment_config()

        # Use hyperparameters passed as 'rl.training.*' to overwrite the default config
        experiment_config_hyperparams_prep = {}
        for key, val in experiment_config["training"].items():
            if key.startswith("rl.training"):
                split_key = key.split(".")
                if len(split_key) == 4:
                    experiment_config_hyperparams_prep[split_key[-2]][split_key[-1]] = val
                if len(split_key) == 3:
                    experiment_config_hyperparams_prep[split_key[-1]] = val
            else:
                experiment_config_hyperparams_prep[key] = val
        experiment_config_training_no_rl_keys = {
            key: val for key, val in experiment_config_hyperparams_prep.items() if not key.startswith("rl.")
        }

        experiment_config["training"] = experiment_config_training_no_rl_keys

        # Run the actual training
        experiment_results = run_experiments(experiment_config, verbose=3)

        latest_trial = experiment_results[-1]

        # Export the checkpoint to the sagemaker model folder
        shutil.copytree(latest_trial.checkpoint.path, FINAL_MODEL_DIR, dirs_exist_ok=True)

        # Export experiment metadata to the sagemaker model folder
        self.save_experiment_metadata(trial_path=latest_trial.path)

    def train_main(self):
        self.launch()


if __name__ == "__main__":
    SageMakerRayLauncher().train_main()
