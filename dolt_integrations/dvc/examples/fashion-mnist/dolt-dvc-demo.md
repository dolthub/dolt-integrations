
# Introduction

The ML tooling space is changing quickly, budding hundreds of
competing and overlapping narratives of how the future will
look in data-land. Some projects, like Blue River, expand
the capabilities of specific verticals, like autonomous tractors. Others
like Weights and Biases support specific stages in the ML
lifecycle, like training and result tracking.
Other projects like Tecton and Databricks aim to redefine what
MLOps-as-a-platform means at Enterprise-scale, making them harder
to bucket and define.

"Data versioning and reproducibility" has a deceivingly simple scope
that masks the variety and difficulty of data capture, similar to
platform projects.

Consider two versioning tools: Dolt and Data Version Control.

Dolt is a Git-versioned SQL database.
Tabular datasets in Dolt gain the powers of production databases
(scalability, schema reliability, query language, logging) and Git
(row-level commits, diffs, and merges, etc).
Dolt is use-case unopinionated, and optimizes for delivering the
experience of MySQL and Git using a novel storage layer.

Data Version Control (DVC) offers ML project management and workflow
versioning. DVC files committed to Git track data in S3 alongside Git
source code. DVC facilitates a process by which a team can
collaborate between those two mediums, Git and remote object stores.

"Git for Data" and "Data Version Control" are colloquially
synonyms, both are open-source projects, have similar star
counts on Github, and growing developer communities on Discord.
Both tailor towards data scientists seeking reproducibility.
Because Dolt is an opinionated data store, and DVC is an opinionated
process management software, we believe the two support one-another.
The degree to which the two are complementary is difficult to answer
abstractly, so we built an integration between them.

In this blog, we explain how and why you would use Dolt and DVC together
with a Fashion-MNIST demo. We will touch on several data versioning
themes that Dolt and DVC tackle in different ways:
1. Ease of access
2. Remotes and sharing
3. Lineage
4. Reproducibility

# Tutorial

## Overview

We will train an image classifier with model code taken from the
[Tensorflow
docs](https://www.tensorflow.org/tutorials/keras/classification).

We chose this example to showcase Dolt and DVC’s strengths:
1. DVC documents workflows by recording commands and the metadata of files
   synced to remote file-systems.
2. Dolt is a versioned SQL database with novel features for managing
   strictly-typed tabular data.

When data is a mix of CSV/Parquet tables and blobs
(like images), using Dolt and DVC together is interesting because Dolt
does not store arbitrary files well, and DVC likewise does not provide
unique support for tabular data.

We divide input and output dependencies between Dolt and DVC.

In the Dolt corner we store tabular data:
1.Training and testing labels
2.Prediction results (guessed and actual labels)

With DVC we store images and binary blobs:
1. Training and testing images
2. Model weights and class pickle

At the end of our workflow will use DVC to push the trained model to an object store:
```bash
data/model
├── assets
├── saved_model.pb
└── variables
    ├── variables.data-00000-of-00001
    └── variables.index
```

And Dolt to inspect results and associate models with results:
```bash
TODO -- results summary, not rows
> dolt sql -q "select * from predictions limit 5"
+--------+------+--------+
| row_id | pred | actual |
+--------+------+--------+
| 66000  | 1    | 6      |
| 66001  | 2    | 3      |
| 66002  | 6    | 5      |
| 66003  | 4    | 4      |
| 66004  | 1    | 5      |
+--------+------+--------+
```

## Setup

Install dolt
```bash
sudo bash -c 'curl -L https://github.com/dolthub/dolt/releases/latest/download/install.sh | sudo bash'
```

Clone and install DVC's `dolt-integration` branch:
```bash
> git clone git@github.com:iterative/dvc.git
> cd dvc
> dvc checkout dolt/integration
> pip install --user .
```

Initialize a separate demo folder:
```bash
> mkdir -p dvc-dolt-demo/{data/images,scratch/mnist}
> cd dvc-dolt-demo
> git init
> dvc init
```

Finally, let's download training code adapted from [Tensorflow
documentation](https://github.com/tensorflow/docs/blob/master/site/en/tutorials/keras/classification.ipynb).
```bash
> dvc get git@github.com:dolthub/dolt-integrations.git dolt_integrations/dvc/examples/fashion_mnist/main.py
```

## Access Data

### DVC

Downloading images with DVC is similar to using cURL to download a file:
```bash
> dvc get git@github.com:zalandoresearch/fashion-mnist.git data/fashion --out scratch/
> dvc get-url http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz scratch/mnist/
> dvc get-url http://yann.lecun.com/exdb/mnist/t10k-images-idx3-ubyte.gz scratch/mnist/
```

"Adding" a file to DVC records metadata and replaces that filename with a
symlink to a DVC cache elsewhere on our system:
```bash
> cp scratch/mnist/* data/images/
> dvc add data/images
100% Add|█████████████████████|1/1 [00:01,  1.45s/file]

To track the changes with git, run:

	git add data/.gitignore data/images.dvc
```

If we pulled files from an existing DVC repo the add step would be performed automatically.

### Dolt

Creating a database requires a transform if the data is not already a
CSV or similar file:
```python
In [1]: import os
   ...: import gzip
   ...: import numpy as np
   ...: import doltcli as dolt

In [2]: with gzip.open("scratch/mnist/train-labels-idx1-ubyte.gz","r") as f:
   ...:     f.read(8) # first two bytes are padded zeros
   ...:     buf = f.read()
   ...:     labels = np.frombuffer(buf, dtype=np.uint8).astype(np.int64)

In [2]: os.makedirs("mnist", exist_ok=True)
   ...: dolt.Dolt.init("mnist")
   ...: db = dolt.Dolt("mnist")
   ...: n = len(labels)
   ...: dolt.write_columns(db, "labels", dict(row_id=range(n), train=[True]*n, label=labels), primary_key=["row_id"])
   ...: db.sql("select DOLT_COMMIT('-am', 'Add MNIST labels')")
```

Running this yourself is unnecessary. We have pre-computed MNIST and
Fashion MNIST, which can be [viewed on
DoltHub](https://www.dolthub.com/repositories/max-hoffman/mnist) and cloned
locally with:
```bash
> dolt clone max-hoffman/mnist data/labels
```

DVC "adding" a dolt database creates a `.dvc` metadata file, but
bypasses the cache because Dolt has its own storage format:
```bash
dvc add data/labels
100% Add|█████████████████████|1/1 [00:01,  1.63s/file]

To track the changes with git, run:

	git add data/.gitignore data/labels.dvc
```

## Remotes and Sharing

### DVC

We will push files to a local remote folder, but this works similarly with an S3 URL:
```bash
> mkdir -p /tmp/dvc-remote
> dvc add dvc_remote /tmp/dvc-remote
> dvc push data/images -r dvc_remote
```

If a co-worker added new data, we would first use Git to reference
new DVC metadata:
```bash
> git pull
```

then download remotes into our cache:
```bash
> dvc pull -r dvc_remote
```

and finally checkout the new data according to the updated `data/images.dvc` spec:
```bash
> dvc checkout data/images
```

### Dolt

Every command above also works with the Dolt DVC integration:
```bash
> mkdir -p /tmp/dolt-remote
> dvc add dolt_remote /tmp/dolt-remote
> dvc push data/ -r dolt_remote

> git pull

> dvc pull -r dolt_remote

> dvc checkout data
```

## Lineage

Before running our workflow, lets setup one more database for the
prediction results:
```bash
> mkdir -p data/predictions
> cd data/predictions
> dolt init
```

And we are ready to run a training example:
```bash
> dvc run \
    -n train \
    -d data/images \
    -d data/labels \
    -o data/model \
    -o data/predictions \
    python train.py
Epoch 1/10
1875/1875 [==============================] - 2s 1ms/step - loss: 1.5923 - accuracy: 0.5852
Epoch 2/10
1875/1875 [==============================] - 2s 1ms/step - loss: 1.4209 - accuracy: 0.6550
Epoch 3/10
1875/1875 [==============================] - 2s 979us/step - loss: 1.3852 - accuracy: 0.6608
Epoch 4/10
1875/1875 [==============================] - 2s 1ms/step - loss: 1.3795 - accuracy: 0.6590
Epoch 5/10
1875/1875 [==============================] - 2s 1ms/step - loss: 1.3458 - accuracy: 0.6659
Epoch 6/10
1875/1875 [==============================] - 2s 1ms/step - loss: 1.3354 - accuracy: 0.6681
Epoch 7/10
1875/1875 [==============================] - 2s 1ms/step - loss: 1.3177 - accuracy: 0.6686
Epoch 8/10
1875/1875 [==============================] - 2s 1ms/step - loss: 1.3071 - accuracy: 0.6689
Epoch 9/10
1875/1875 [==============================] - 2s 1ms/step - loss: 1.2764 - accuracy: 0.6741
Epoch 10/10
1875/1875 [==============================] - 2s 998us/step - loss: 1.2752 - accuracy: 0.6725
313/313 - 0s - loss: 1.7931 - accuracy: 0.5620

```

A `dvc run` command translates to a stage in our `dvc.yaml` workflow file:
```bash
> cat dvc.yml
schema: '2.0'
stages:
  train:
    cmd: python train.py
    deps:
    - path: data/images
      md5: 9619f1bb9b50d0195a0af00656fb8bf4.dir
      size: 11594788
      nfiles: 5
    - path: data/labels
      md5: tkcao72e0upe2umb2kokartgivd9keqc-pinisl8m4tfdecb8iikqhicta61nou9s-.dolt
      size: 1463348
    outs:
    - path: data/model
      md5: e0ac4164a31880e68ec64eb6db1a6507.dir
      size: 1303322
      nfiles: 3
    - path: data/predictions
      md5: t65aqoe5ftkaqlkepqoaabnkq77mp8kn-of48sjsfirdudglo6m444fpqnsaddbhb-.dolt
      size: 170002
```

DVC includes helper commands to visualize the pipeline:
```bash
> dvc dag
+-----------------+         +-----------------+
| data/images.dvc |         | data/labels.dvc |
+-----------------+         +-----------------+
                **            **
                  **        **
                    **    **
                   +-------+
                   | train |
                   +-------+
```

## Reproducibility

As a last step we will swap MNIST for the structurally-identical
Fashion-MNIST.

First, we swap the DVC image files:
```bash
> cp scratch/fashion/* data/images
```

On the Dolt-side we checkout the fashion branch:
```bash
> cd fashion-mnist
> dolt checkout fashion
> cd ..
```

DVC picks up on these changes:
```bash
> dvc status
train:
	changed deps:
		modified:           data/images
		modified:           data/labels
data/labels.dvc:
	changed outs:
		modified:           data/labels
data/images.dvc:
	changed outs:
		modified:           data/images
```

Which we can fix:
```bash
> dvc add data/images --quiet
> dvc add fashion-mnist --quiet
```

DVC re-triggers our workflow to generate new outputs to match the
updated fashion images and labels:
```bash
> dvc repro
'data/images.dvc' didn't change, skipping
'data/labels.dvc' didn't change, skipping
> python train.py
Epoch 1/10
1875/1875 [==============================] - 2s 1ms/step - loss: 1.6889 - accuracy: 0.5248
Epoch 2/10
1875/1875 [==============================] - 2s 1ms/step - loss: 1.5610 - accuracy: 0.5818
Epoch 3/10
1875/1875 [==============================] - 2s 986us/step - loss: 1.5356 - accuracy: 0.5898
Epoch 4/10
1875/1875 [==============================] - 2s 1ms/step - loss: 1.5066 - accuracy: 0.6006
Epoch 5/10
1875/1875 [==============================] - 2s 1ms/step - loss: 1.4952 - accuracy: 0.6038
Epoch 6/10
1875/1875 [==============================] - 2s 1ms/step - loss: 1.4926 - accuracy: 0.6029
Epoch 7/10
1875/1875 [==============================] - 2s 1ms/step - loss: 1.4769 - accuracy: 0.6067
Epoch 8/10
1875/1875 [==============================] - 2s 1ms/step - loss: 1.4548 - accuracy: 0.6127
Epoch 9/10
1875/1875 [==============================] - 2s 1ms/step - loss: 1.4657 - accuracy: 0.6100
Epoch 10/10
1875/1875 [==============================] - 2s 1ms/step - loss: 1.4414 - accuracy: 0.6138
313/313 - 0s - loss: 1.5174 - accuracy: 0.6027
Updating lock file 'dvc.lock'

To track the changes with git, run:

	git add dvc.lock
Use `dvc push` to send your updates to remote storage.
```

# Technical Discussion

## After Training

Data science iterations usually benefit from tracking differences between
input and output datasets.

MINST and Fashion-MNIST are completely different sets of images, but
random chance leaves about 1/N labels the same between the two that
Dolt opportunistically deltas:
```bash
> dolt diff fashion --summary
diff --dolt a/labels b/labels
--- a/labels @ h6a5mj4rle852jj2133dtl0ofi6hdsqf
+++ b/labels @ bcvu38tkg94vmvua2qi1c9rpsg4tktpn
7,038 Rows Unmodified (10.05%)
0 Rows Added (0.00%)
0 Rows Deleted (0.00%)
62,962 Rows Modified (89.95%)
62,962 Cells Modified (29.98%)
(70,000 Entries vs 70,000 Entries)
```

The motivation behind Fashion-MNIST, that models overfit digits and shirts
are generally more difficult to categorize, can be seen by comparing
accuracies between the training runs:
```bash
TODO use history to create correct or fail, then accuracy, test cnt, category_num
> dolt sql -q “with”
```

A stored procedure might make it easier to automate tracking performance
without having to query the entire commit history:
```bash
TODO dolt add and invoke procedure for the above
```

One instance where we might want to access the commit history is for
`dolt blame`’ing a data changes. For example, identifying who added
the fashion branch:
```bash
TODO find who added the fashion branch
```

You can learn more ways to use Dolt’s Git and SQL features
[here](https://docs.dolthub.com).

## How Dolt Integrates With DVC

### Adding

DVC’s main piece of metadata is a file-hash used to retrieve object versions.
Usually, DVC hashes files and directories the same way as Git. To avoid
duplicating what Dolt does internally, we substituted Dolt’s three-heads
in-lieu of an md5 hash. Dolt
heads tell us 1) the last commit, 2) the ongoing index/staging commit hash,
and 3) the commit hash if all files in our working directory were committed
now. Three-heads let us monitor whether the current database has changed
the most recent DVC-add, and if necessary, checkout
the appropriate database version.

##sCommits

In DVC, output lineage is captured as Git-committed YAML files.
Pre-defined output paths are saved as-is when a workflow completes.

In Dolt, a user decides when to commit changes.
If a new database state is committed within a workflow,
DVC will record the new commit.
If a tracked database is changed but not committed by the end of a
workflow, then we have created an uncommitted transaction, a state that
Dolt cannot reproduce.
```bash
TODO fix and show error for dirty status on add
```

Fixing this error requires committing changes.
Reaping the benefits of Dolt requires making state changes in the
versioning format that Dolt persists, commits.

### Remotes

There are several differences between Dolt and DVC remotes
1. A DVC remote is a content-addressed-store, while the Dolt-remote
   maintains a commit history and database.
2. A Dolt remote could be hosted on DoltHub, in the same way Git
   remotes are often hosted on GitHub.
3. DVC and Dolt will store internal references to remotes created by DVC
   commands.

# Conclusion

Dolt excels at reproducibility for tabular data because commits,
branches, merges and diffs are its fundamental building blocks.

DVC creates a process by which data scientists can better organize their
work and collaborate.

todo -- summary

todo -- call to action

