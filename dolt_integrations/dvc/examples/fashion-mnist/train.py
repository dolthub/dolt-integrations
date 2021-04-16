import datetime
import gzip
import os

import doltcli as dolt
import numpy as np
import tensorflow as tf
import keras

def get_data(label_path, image_path):
    # get labels from Dolt database
    labeldb = dolt.Dolt(label_path)
    train_labels_sorted = dolt.read_columns_sql(
        labeldb,
        "select label from labels where train = 1 order by row_id",
    )["label"]
    test_labels_sorted = dolt.read_columns_sql(
        labeldb,
        "select label from labels where train = 0 order by row_id",
    )["label"]

    # type-convert labels
    train_labels = np.array(train_labels_sorted).astype(np.int32)
    test_labels = np.array(test_labels_sorted).astype(np.int32)

    # constants for restoring image shapes
    image_size = 28
    train_num = 60000
    test_num = 10000

    # read train and test images
    with gzip.open(os.path.join(image_path, "train-images-idx3-ubyte.gz"),"r") as f:
        f.read(16)
        buf = f.read()
        data = np.frombuffer(buf, dtype=np.uint8).astype(np.int32)
        train_images = data.reshape(train_num, image_size, image_size, 1)

    with gzip.open(os.path.join(image_path, "t10k-images-idx3-ubyte.gz"),"r") as f:
        f.read(16)
        buf = f.read()
        data = np.frombuffer(buf, dtype=np.uint8).astype(np.int32)
        test_images = data.reshape(test_num, image_size, image_size, 1)

    return (train_images, train_labels), (test_images, test_labels)

def train():

    # path parameters
    image_path = "data/images"
    label_path = "data/labels"
    model_path = "data/model"
    prediction_path = "data/predictions"

    # verify clean database states
    preddb = dolt.Dolt(prediction_path)
    if not preddb.status().is_clean:
        raise ValueError("Prediction database state must be clean to start a run.")

    labeldb = dolt.Dolt(label_path)
    if not preddb.status().is_clean:
        raise ValueError("Label database state must be clean to start a run.")

    (train_images, train_labels), (test_images, test_labels) = get_data(label_path, image_path)

    # pre-process images
    train_images = train_images / 255.0
    test_images = test_images / 255.0

    # define simple multi-layer neural net
    model = keras.Sequential([
        keras.layers.Flatten(input_shape=(28, 28)),
        keras.layers.Dense(128, activation="relu"),
        keras.layers.Dense(10)
    ])
    model.compile(
        optimizer="adam",
        loss=keras.losses.SparseCategoricalCrossentropy(from_logits=True),
        metrics=["accuracy"],
    )

    # train model
    model.fit(train_images, train_labels, epochs=10)
    model.save(model_path)

    # evaluate model
    probability_model = keras.Sequential([model, keras.layers.Softmax()])
    test_loss, test_acc = model.evaluate(test_images,  test_labels, verbose=2)
    predictions = np.argmax(probability_model.predict(test_images), axis=1)

    # record outputs
    outputs = dict(
        row_id=range(len(train_labels), len(train_labels)+len(test_labels)),
        pred=predictions.tolist(),
        actual=test_labels.tolist(),
    )
    dolt.write_columns(preddb, "predictions", outputs, primary_key=["row_id"])

    ts = datetime.datetime.now()
    label_branch = labeldb.sql("select active_branch() as ab", result_format="csv")[0]["ab"]
    summary = dict(
        loss=test_loss,
        acc=test_acc,
        timestamp=ts,
        label_branch=label_branch,
    )
    dolt.write_rows(preddb, "summary", [summary], primary_key=["timestamp"])
    preddb.sql(f"select dolt_commit('-am', 'New workflow run at {ts}')")

if __name__=="__main__":
    train()
