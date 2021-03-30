
# Introduction

"Git for Data" and "Data Version Control" are colloquially
synonyms, both are open-source projects, have similar star
counts on Github, and growing developer communities on Discord.
Both tailor towards data scientists seeking reproducibility.
To what degree do naming similarities bely technical differences in
the two projects?
More importantly, what is the right way to compare two reproducibility tools?

# Background

The ML tooling space is changing quickly, budding hundreds of
competing and overlapping narratives of how the future will
look in data-land. Some projects, like Blue River, expand
the capabilities of specific verticals, like autonomous tractors. Others
like Weights and Biases support specific stages in the ML
lifecycle, like training and result tracking.
Other projects like Tecton and Databricks aim to redefine what
MLOps-as-a-platform means at Enterprise-scale, making them much harder
to bucket and define.

"Data versioning and reproducibility" has a decievingly simple scope
that masks the variety and difficulty of data capture, similar to
platform projects. At Dolt, we often get asked how to comapre two reproducibility
projects that claim similar goals. Today, we will take a slice of that
puzzle and put it under the microscope in the context of an ML
workflow.

We built an integration between DVC and Dolt to help compare the
two along several dimensions:
1. Ease of access
2. Remotes and sharing
3. Lineage
4. Reproducibility

Pairing Dolt and DVC together highlights one big point that we hope to
show: DVC and Dolt can work together, because the features provided by each are
unique and non-redundant

# Tutorial

## Overview

To help showcase the two in-action, we will use a tutorial from the
[Tensorflow
docs](https://github.com/tensorflow/docs/blob/master/site/en/tutorials/keras/classification.ipynb).

The reason we chose this example, to give a TLDR of the blog, is that:
1. DVC documents workflows by recording commands and metadata of files
   that can be synced between local workspaces and remote file-systems.
2. Dolt is a versioned SQL database with novel features for managing
   strictly-typed tabular data.

In the instances where data is a mix of CSV/Parquet tables and blobs
(like images), using Dolt and DVC together is interesting because Dolt
does not store arbitrary files well, and DVC likewise does not provide
unique support for tabular data. Together they support each-others blind
spots.

## Setup

Clone and install DVC's `dolt-integration` branch:
```bash
> git clone git@github.com:iterative/dvc.git
> cd dvc
> dvc checkout dolt/integration
> pip install --user .
```

Initialize a separate demo folder:
```bash
> mkdir -p dvc-dolt-demo/data
> cd dvc-dolt-demo
> git init
> dvc init
```

TODO: make this
Finally, let's download training source-code adapted from the [offical Tensorflow
documentation](https://github.com/tensorflow/docs/blob/master/site/en/tutorials/keras/classification.ipynb).
```bash
> dvc get git@github.com:dolthub/dolt-integrations.git dolt_integrations/dvc/examples/fashion_mnist/main.py
```

## Ease of Access

### DVC

Downloading data with DVC is similar to using cURL to download a file:
```bash
> dvc get git@github.com:zalandoresearch/fashion-mnist.git data/fashion --out data/
> dvc get git@github.com:zalandoresearch/fashion-mnist.git data/mnist --out data/
```

"Adding" a file to DVC records metadata and replaces that filename with a
symlink to a DVC cache elsewhere on our system:
```bash
> dvc add data
```

Pulling files from existing DVC repos combines the steps, automatically
symlinking the file from our cache.
TODO

### Dolt

Cloning data from Dolt is simple after the database has been created:
```bash
> dolt clone max/mnist
```

Creating a database requires a transform if the
data is not already a CSV or similar file, like MNIST:
```
TODO
```

DVC adding a dolt database creates a `.dvc` metadata file, but
bypasses the cache because Dolt has its own storage format:
```bash
dvc add max/mnist
```

Notice that the metadata hash of our Dolt database is not a standard
hash of the `.dolt` directory. The Dolt hash is instead the "three
heads" that uniquely describe the state of our database, in the same way
Git commits describe unique repos.
```
TODO
```

### Comparison

At the end of every DVC download is a URL pointing to a file. Dolt is
cloned as a database in chunks similar to Git. Dolt clones can also be shallow,
downloading a slice of commits or data.

Getting files into a Dolt database is a process. That process requires
work, but also cleans and organizes schemas for your data.

## Remotes and Sharing

### DVC

We will push files to a local remote folder:
```bash
> mkdir -p /tmp/dvc-remote
> dvc add dvc_remote /tmp/dvc-remote
> dvc push data/ -r dvc_remote
```

If a co-worker adds new data, we will need to checkout the updates.
First we sync our `.dvc` files from Git, referencing the new metadata:
```bash
> git pull
```

then download remotes into our cache:
```bash
> dvc pull -r dvc_remote
```

and finally checkout the new data according to the `.dvc` spec:
```bash
> dvc checkout data
```

### Dolt

Every command above also works for Dolt with the integration:
```bash
> mkdir -p /tmp/dolt-remote
> dvc add dolt_remote /tmp/dolt-remote
> dvc push data/ -r dolt_remote

> git pull

> dvc pull -r dolt_remote

> dvc checkout data
```

### Comparison


There are several differences:
1. The remote contents are structurally different. A DVC
   remote is a content-addressed-store, while the Dolt-remote holds
   the commit history and database.
2. A Dolt remote could be hosted on DoltHub, in the same way Git
   remotes are often hosted on GitHub.
3. DVC holds references to remotes, but Dolt also stores
   internal references of remotes created by DVC commands:
```
TODO
```

## Lineage

Before running our workflow lets setup a result database:
```bash
> mkdir mnist-results
> cd mnist results
> dolt init
```

Our training script references images stored in DVC:
```
TODO
```

and labels stored in Dolt:
```
TODO
```

writing outputs to a Dolt database:
```
TODO
```

Now we can train a model with DVC to capture the workflow
state:
```bash
> dvc run -n train ...
> dvc run -n evalutate ...
```

Each `dvc run` command translates to a stage in our `dvc.yaml` workflow file:
```
TODO
```

DVC includes helper commands to visualize the pipeline:
```bash
> dvc run
```

### Comparison

With DVC, output lineage is captured as Git-committed metadata files.
At the end of the workflow, pre-defined output paths are saved as-is.

Dolt does not decide for a user when to commit
database changes. As long as a new database state is committed inside
the script, DVC will record the final state:
```
TODO
```

Fine-tuning commits in Dolt is either a blessing or a curse,
depending on whether you want the features associated with
commits, merges and diffs.

## Reproducibility

Our last example will swap MNIST for the structurally-similar
Fashion-MNIST.

First, we swap the DVC data files:
```bash
> dvc rm data/...
> cp scratch/fashion/ data/...
> dvc add data/...
```

On the Dolt-side we checkout the fashion branch:
```bash
> cd fashion-mnist
> dolt checkout fashion
> cd ..
> dvc add fashion-mnist
```

DVC can now re-trigger our workflow to generate new outputs to match the
new fashion images and labels:
```bash
> dvc repro
```

### Comparison

We touched on how remotes and sharing work in DVC and Dolt earlier.
What we are interested here is how DVC and Dolt manage data changes,
one of the core product goals of a data versioning software.

DVC is a primary source for caching multiple copies of files.
To make sense of data changes in DVC you either use Git-commits as a
proxy for pulling data changes, organize your own file-heirarchy, or
manually compare different versions of files from the object store.

Dolt requires more work, because you have to organize data into rows
fitting a schema. Users also must decide when data changes
become a "commit". In many cases, understading your data's structure and
using commit deltas instead of copying files is a feature, not a bug.

Dolt SQL lets us summarize results:
TODO

`dolt diff` shows us what changed between MNIST and Fashion-MNIST:
TODO

`dolt blame` shows who made changes:

Dolt excels at reproducibility for tabular data because commits,
branches, merges and diffs are its fundamental building blocks.

# Conclusion

todo -- summary

todo -- call to action
