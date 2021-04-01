import gzip

import doltcli as dolt
import numpy as np
import tensorflow as tf
import keras

def get_data():
    doltdb = dolt.Dolt("data/mnist")
    train_labels = np.array(dolt.read_columns_sql(doltdb, "select label from labels where train = 1 order by row_id")["label"]).astype(np.float32)
    test_labels = np.array(dolt.read_columns_sql(doltdb, "select label from labels where train = 0 order by row_id")["label"]).astype(np.float32)

    image_size = 28
    train_num = 60000
    test_num = 10000

    with gzip.open("data/images/train-images-idx3-ubyte.gz","r") as f:
        f.read(16)
        buf = f.read()
        data = np.frombuffer(buf, dtype=np.uint8).astype(np.float32)
        train_images = data.reshape(train_num, image_size, image_size, 1)

    with gzip.open("data/images/t10k-images-idx3-ubyte.gz","r") as f:
        f.read(16)
        buf = f.read()
        data = np.frombuffer(buf, dtype=np.uint8).astype(np.float32)
        test_images = data.reshape(test_num, image_size, image_size, 1)

    return (train_images, train_labels), (test_images, test_labels)

def train():
    (train_images, train_labels), (test_images, test_labels) = get_data()
    print(type(train_images[0]), type(train_labels[0]))

    train_images = train_images / 255.0

    test_images = test_images / 255.0

    model = keras.Sequential([
        keras.layers.Flatten(input_shape=(28, 28)),
        keras.layers.Dense(128, activation='relu'),
        keras.layers.Dense(10)
    ])

    model.compile(
        optimizer='adam',
        loss=keras.losses.SparseCategoricalCrossentropy(from_logits=True),
        metrics=['accuracy'],
    )

    model.fit(train_images, train_labels, epochs=1)

    test_loss, test_acc = model.evaluate(test_images,  test_labels, verbose=2)

    probability_model = keras.Sequential([model, keras.layers.Softmax()])
    predictions = np.argmax(probability_model.predict(test_images), axis=1)
    print(len(predictions.tolist()), test_labels.shape, len(range(60000, 70000)))

    outputdb = dolt.Dolt("data/predictions")
    outputs = dict(
        row_id=range(60000, 70000),
        pred=predictions.tolist(),
        actual=test_labels.tolist(),
    )
    dolt.write_columns(outputdb, "predictions", outputs, primary_key=["row_id"])

if __name__=="__main__":
    train()
