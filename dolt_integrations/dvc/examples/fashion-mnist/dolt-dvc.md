# Dolt DVC Integration

## Overview

## Setup

Clone and install DVC's dolt-integration branch:
```bash
> git clone git@github.com:iterative/dvc.git
> cd dvc
> dvc checkout dolt/integration
> pip install --user .
```

Initialize a separate demo folder:
```bash
> mkdir dvc-dolt-demo
> cd dvc-dolt-demo
> git init
> dvc init
```

Setup a scratch folder for un-tracked data:
```bash
> mkdir data
> echo "data/" >> .gitignore
```

Finally, let's download some source-code adapted from the [offical Tensorflow
# TODO: make this
documentation](https://github.com/tensorflow/docs/blob/master/site/en/tutorials/keras/classification.ipynb).
```bash
> dvc get git@github.com:dolthub/dolt-integrations.git dolt_integrations/dvc/examples/fashion_mnist/main.py
```

## Ease of Access

### DVC

DVC can load data from URLs:
# TODO: download MNIST using DVC get
```bash
> dvc get git@github.com:zalandoresearch/fashion-mnist.git data/fashion --out data/
> dvc get git@github.com:zalandoresearch/fashion-mnist.git data/mnist --out data/
```

DVC projects can also be implicitly pulled via
[Git-tracked DVC metadata](https://dvc.org/doc/command-reference/import)
from existing DVC projects.

DVC add kicks-off two actions:

1. Create metadata file for dependency file/folder.
2. Move file/folder contents into a content-addressed store, replacing
   the source files with symlinks.
```bash
> dvc add data/mnist/train-images-idx3-ubyte
```

### Dolt

DoltHub repos can be cloned with Git's syntax:
```bash
> dolt clone fashion-mnist
```

Our integration creates metadata files for Dolt databases on a `dvc
add`. Instead of hashing the database, we use the HEAD, staging and
working hashes to track status changes.
```bash
dvc add fashion-mnist
```

### Summary

At the end of a DVC download is a URL pointing to a file. Dolt does not
share that one-to-one mapping, and is cloned as a database in chunks
similar to Git. Dolt clones can be shallow, downloading a slice of
commits or data, but a standard `dolt clone` will download an entire
database.

## Data Sharing and Remotes

So far we have ignored Git's two-sided push/pull model. What happens
when our data changes or we want to share progress with
colleages?

### DVC
Change file checkout. Add and push to remote.

### Dolt
Change row checkout. Add and push to remote.

### Summary

DVC data is one-step removed from a slice of metadata checked into Git.
In our integration, Dolt databases can be shared the same way, but
is ultimately a separate system from Git and could be pushed/pulled
separately. Storing Dolt hashes in DVC metadata files is necessary
for workflow reproducibility with a tool like DVC.

## Lineage and Reproducibility

### MNIST

Before running our workflow, we will add one more repo
for output results:
```bash
> mkdir mnist-results
> cd mnist results
> dolt init
```

Now we can use DVC to track the training pipeline through evaluation:
```bash
> dvc run -n train ...
> dvc run -n evalutate ...
```

DVC helper functions can visualize our work:
```bash
> dvc dag
```

### Fashion MNIST

What happens when input and output dependencies change for
an established process? How do we update data, track changes, and
send results to collaborators?

We will swap our original dataset with Fashion-MNIST and see what
happens downstream. FMNIST is format-compatable with MNIST, and this
should similate something similar to mutating or pre-processing,
but now swapping clothes for letters.

On the Dolt-side, we will checkout the fashion branch:
```bash
> cd fashion-mnist
> dolt checkout fashion
> cd ..
> dvc add fashion-mnist
```

On the DVC-side, we will substitute the fashion images:
```bash
> dvc rm data/...
> cp scratch/mnist/ data/...
> dvc add data/...
```

We have told DVC that our input dependencies have changed,
and can trigger a re-run to generate new outputs:
```bash
dvc repro
```

We can commit new metadata hashes into Git, `dvc push` our
input results to a shared location, and our collaborators can replicate
our work.

As a bonus, Dolt recorded everything about the differences between the
input and output labels:

1. `dolt diff` tells us the entire table was re-written:
2. `dolt blame` shows us where the new data came from:
3. We can use SQL to quickly compare the results without using an
  interactive Pandas session:


## Summary

We covered a lot of ground training a Fashion-MNIST classifier
with Dolt and DVC. Dolt and DVC sound confusingly
similar at first-glance. Integrating Dolt as a datasource for
DVC pipelines should help clarify the strenths of each respective tool.

DVC provides a process for versioning data science workflows with
YAML files checked-in to Git, and is great for pushing images and other
files to/from remote object stores. Dolt is an database that leverages
Git concepts to provide data versioning at the storage layer.
Dolt provides a much finer-grained style of vesioning, diffing, merging
and logging for tabular data than wholesale object storage.
