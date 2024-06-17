import tensorflow as tf

gpu_devices = tf.config.list_physical_devices("GPU")

print("GPU_DEVICES: ", gpu_devices)
