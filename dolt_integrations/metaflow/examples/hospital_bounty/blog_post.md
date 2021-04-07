---
title: Introducing Dolt + Metaflow
---

## Background
This post details how to use Metaflow with Dolt. [Metaflow](https://metaflow.org/) is a workflow manager that offers data scientists the ability to define local experiments and scale those experiments to production jobs from a single API. [Dolt](https://docs.dolthub.com/) is a version controlled relational database. It provides a familiar SQL interface along with Git-like version control features. Each commit corresponds to a complete state of the database at the time the commit was created. Both Dolt and Metaflow are open source.

To illustrate the power of integrating Metaflow and Dolt, we use an example Metaflow pipeline to produce a Pandas DataFrame stored in a Dolt database for use by application layer services. The goal is for application layer code to read from datasets that are versioned and reproducible to create a more healthy boundary between data science production services using Dolt's version control features:
![Data Architecture](dolt-metaflow-integration-data-architecture.png)

This blog post will breakdown how to use Dolt to augment Metaflow based pipelines with full reproducibility, lineage, and back-testing capabilities for tabular data read from and written to Dolt. Dolt allows users to see where their data came from, what it looked like at every transformation, and to feed historical versions of it into subsequent flow runs.

## Example Pipeline
Our pipeline consists of two flows. One flow consumes the results written by the other. The first flow computes the state level median price for a hospital procedure. The second flow computes the variance of the price for a procedure across states. 

![image here](dolt-metaflow-pipeline.png)

We will use the end result to illustrate how integrating Dolt and Metaflow provides Metaflow users with the ability to traverse versions of their final data, as well as traceback through the pipeline to examine various stages. Users can do this via the Metaflow API and Dolt integration in a familiar environment like a console or a notebook.

## How it Works
Our design goal with this integration was to give the Metaflow user additional capabilities directly from Metaflow. We wanted to minimize the additional API surface area required.

Workflows in Metaflow are called "flows." Each flow stores metadata about flow execution, referred to as a "run." Each time a run interacts with Dolt it captures a small amount of metadata that makes that interaction reproducible. Since Dolt database root hashes are unique, this creates a mapping (roughly) between Metaflow runs and Dolt commits that can be exploited to provide users with powerful lineage and reproducibility features. 

At the level of the Flow, the integration looks like this, with the runs of a flow reading from and writing Pandas DataFrame objects to and from Dolt:

![Flow level view](dolt-metaflow-integration-flow-level.png)

Drilling into one of the runs, the individual steps can create separate commits that snapshot the state of the database they create:

![Run level view](dolt-metaflow-integration-run-level.png)

When a flow reads data from Dolt, it records exactly how that data was read inside Metaflow. When a flow writes to Dolt it creates a commit and captures the metadata, as well as formatting the commit message. This allows users to browse the inputs and outputs of their flows from the Metaflow API directly without having to know much of anything about Dolt. Furthermore users can retrieve the flow that last touched a table at a given branch or commit, also directly from the Metaflow API. 

This is all abstract, so let's install a few dependencies, grab a dataset, and get stuck into running our pipeline.

## Setup
Let's get the boring stuff out of the way. We need the following:
- Dolt and `doltpy-integrations` installed
- Metaflow installed
- the sample dataset we will use, which can easily clone from DoltHub

### Install Dolt
The first step is to install Dolt on a `*nix` system:
```
sudo bash -c 'curl -L https://github.com/dolthub/dolt/releases/latest/download/install.sh | bash'
```

There are Windows distributions and a Homebrew cask. Find more details about installation [here](https://docs.dolthub.com/getting-started/installation).

### Install doltpy-integrations[metaflow]
Next let's install the Metaflow + Dolt integration. It comes packaged with both Metaflow, and Dolt's Python API, Doltcli. It's easy enough to install via `pip`:
```
pip install doltpy-integraions[metaflow]
```

### Get The Data
The final step is to acquire the dataset. Becuase Dolt is a SQL database with Git-like version control features, it includes the ability to clone a remote to your local machine. We can use that feature to easily acquire a dataset:
```
$ dolt clone dolthub/hospital-price-transparency && cd hospital-price-transparency
```

Note this dataset is nearly 20 gigabytes, and could take a few minutes to clone. Once it's landed it's straightforward to jump right into SQL:
```
$ dolt sql
# Welcome to the DoltSQL shell.
# Statements must be terminated with ';'.
# "exit" or "quit" (or Ctrl-D) to exit.
hospital_price_transparency> show tables;
+-----------+
| Table     |
+-----------+
| cpt_hcpcs |
| hospitals |
| prices    |
+-----------+
```

Finally let's create a Dolt database for our downstream results:
```
$ mkdir ~/hospital-price-analysis && cd ~/hospital-price-analysis
$ dolt init
Successfully initialized dolt data repository.
```

We are now ready to start running our Metaflow based pipeline.

## Using Metaflow
Before we get into the details, let's first produce a run of our pipeline off the latest version of the upstream database. The first flow computes the median cost of a given hospital procedure at the state level:
```
$ poetry run python3 hospital_procedure_price_state_medians.py run \ 
    --hospital-price-db path/to/hospital-price-transparency \ 
    --hospital-price-analysis-db path/to/hospital-price-analysis
```

The second flow computes the variance in median procedure price across states:
```
$ poetry run python3 hospital_procedure_price_variance_by_state.py run \ 
    --hospital-price-analysis-db path/to/hospital-price-analysis
```

We now have our first result set computed. Let's access the computed variances via the integration, using the flow as an entry point:
```python
from metaflow import Flow
from dolt_integrations.metaflow import DoltDT
dolt = DoltDT(run=Flow('HospitalProcedurePriceVarianceByState').latest_successful_run)
df = dolt.read('variance_by_procedure')
print(df)
```

We see that we have successfully computed procedure level variance:
```
                    code         price
0        CPT® 83520,1700  5.146559e+03
1        CPT® 83520,1701  5.146559e+03
2        CPT® 83520,1702  5.146559e+03
3        CPT® 83520,1703  5.146559e+03
4        CPT® 83520,1704  5.146559e+03
                  ...           ...
1314269           nan,13  2.057771e+06
1314270            nan,2  1.269626e+05
1314271            nan,3  8.994087e+03
1314272            nan,4  1.498617e+05
1314273            nan,5  2.541488e+05
[1314274 rows x 2 columns]
```

We have seen it's relatively straightforward to run our pipeline, and access our versioned results via a reference to the flow that produced them. We now dive into some of the capabilities this provides Metaflow users who choose to use Dolt in their infrastructure.

### Back-testing
In this example our input dataset is stored in Dolt. We used a DoltHub dataset because it's easy to clone the dataset and get started, and afterall this post is about integrating Dolt with Metaflow. But having our input dataset in Dolt isn't just a matter of convenience for this post. Because every Dolt commit represents the complete state of the database at a point in time, we can actually easily point our pipeline to historical versions of the data. Let's examine the Dolt commit graph and grab a commit straight from the SQL console:
```
$ cd path/to/hospital-price-transparency
$ dolt sql
# Welcome to the DoltSQL shell.
# Statements must be terminated with ';'.
# "exit" or "quit" (or Ctrl-D) to exit.
hospital_price_transparency> select commit_hash, message from dolt_commits where `date` < '2021-02-17' order by `date` desc limit 10;
+----------------------------------+------------------------------------------------------------+
| commit_hash                      | message                                                    |
+----------------------------------+------------------------------------------------------------+
| f0lecmblorr67rcuhuti6tbkriigh6gt | Updating prices with changes from uwmc_prices.csv          |
| mj9ce6d8em9avj9ej0pqnaoes4fbglti | Updating cpt_hcpcs with changes from uwmc_cpt_hcpcs.csv    |
| 2j6ommult20qvbj05j1nq63nkbd5fgdj | Updating prices with changes from prices.csv               |
| pu8ctvhfcpp83q3iil8trp90vnuesaci | Updating cpt_hcpcs with changes from cpt_hcpcs.csv         |
| q49l0kgnbbbgkt3imjd57tslbi2iges8 | Updating hospitals with changes from hospitals.csv         |
| gstcq5loi9ieqdv1elrljab9hcgr090p | Updating hospitals with changes from hospitals.csv         |
| te6spcqtjk0scose2c45f9t7tpcrt69c | Added hospital WellSpan Surgery & Rehabilitation Hospital. |
| bjg3b5lua8omadcl5nr6o7v0nphliqpu | Added hospital WellSpan York Hospital.                     |
| t4js1g5mfvgikqlmqa238it26mg94g5i | Added hospital Children's of Alabama.                      |
| jsan7p4iad61cjmeti858ebcl4s86vda | Added hospital McLaren Lapeer Region.                      |
+----------------------------------+------------------------------------------------------------+
```

Suppose we'd like to run our pipeline with input data as of commit `gstcq5loi9ieqdv1elrljab9hcgr090p`. That's easy enough, first let's name the commit with a branch:
```
$ dolt branch metaflow-backtest gstcq5loi9ieqdv1elrljab9hcgr090p
```

Now let's kick off recomputing the medians. Since we are recomputing our medians from a historical version of the raw pricing data we will write them to a separate experimentation branch:
```
$ poetry run python3 hospital_procedure_price_state_medians.py run \ 
    --hospital-price-db path/to/hospital-price-transparency \ 
    --hospital-price-db-branch metaflow-backtest \
    --hospital-price-analysis-db path/to/hospital-price-analysis \
    --hospital-price-analysis-db-branch metaflow-backtest
```

And the variances can be run similarly, again using the experimentation branch:
```
$ poetry run python3 hospital_procedure_price_variance_by_state.py run \ 
    --hospital-price-analysis-db path/to/hospital-price-analysis \
    --hospital-price-analysis-db-branch metaflow-backtest
```

We can now query the results directly from Python:
```python
from metaflow import Flow
from dolt_integrations.metaflow import DoltDT
dolt = DoltDT(run=Flow('HospitalProcedurePriceVarianceByState').latest_successful_run)
df = dolt.read('variance_by_procedure')
print(df)
```

Or we can use Dolt SQL to query the results directly:
```
$ cd path/to/hospital-price-analysis
$ dolt sql
# Welcome to the DoltSQL shell.
# Statements must be terminated with ';'.
# "exit" or "quit" (or Ctrl-D) to exit.
hospital_price_analysis> 
```

In this section we saw how storing flow inputs in Dolt makes back-testing straightforward. Dolt's commit graph makes it straight forward to specify a historical database state. 

### Reproducibility
Our pipeline contains two steps, one computes state procedure price medians, and the second computes procedure price variances across states. Suppose now that we would like to tweak the way we compute variances. We might like to exclude some outliers, or invalid procedure codes. In a production setting we might own the variances computation but not the medians computation, and have stricter criteria for excluding invalid data. Let's update our variances job and then recompute using a fixed.

Let's first look at our Flow definition to exclude corrupt procedure codes:
```python
@step
def start(self):
    analysis_conf = DoltConfig(
        database=self.hospital_price_analysis_db,
        branch=self.hospital_price_analysis_db_branch
    )

    with DoltDT(run=self.historical_run_path or self, config=analysis_conf) as dolt:
        median_price_by_state = dolt.read("state_procedure_medians")
        clean = median_price_by_state[median_price_by_state['code'].str.startswith('nan')]
        variance_by_procedure = clean.groupby("code").var()
        dolt.write(variance_by_procedure, "variance_by_procedure")

    self.next(self.end)

```

Reproducibility comes from passing the path to a previous run:
```python
historical_run_path = Parameter(
        "historical-run-path", help="Read the same data as a path to a previous run"
    )
```

We use that path as a parameter to `DoltDT`, which in turn causes `DoltDT` to read data in exactly the same way as the run specified by the provided run path:
```python
with DoltDT(run=self.historical_run_path or self, config=analysis_conf) as dolt:
```

Let's kick off this reproducible run:
```
$ poetry run python3 hospital_procedure_price_variance_by_state.py run \ 
    --hospital-price-analysis-db path/to/hospital-price-analysis
    --hospital-price-analysis-db-branch metaflow-change-test
    --historical-run-path <path/to/metaflow/run>
```

We can now perform a diff between the two resutls:
```
show diff
```

By simply retrieving a run path, and kicking off our variances flow, we were able to reproduce the exact inputs of a historical run. We were able to directly diff the data produced from the Metaflow API used alongside the integration.

### Lineage
In the previous section we showed how to run one Flow using the inputs of a previous run. We did this to achieve data version isolation for the purposes of testing our code changes. The same mechanism we used for achieving this kind of reproducibility also allows us to track data lineage. Recall that our pipeline has two steps, each a separate flow:

![High Level Pipeline](dolt-metaflow-job-structure.png)

Obviously a real world example might have a much more complicated data dependency graph, making this kind of tracking all the more important. Let's see how we would trace the lineage of the final variances. The first thing to do is grab the run that created the current production data:

```python
from dolt_integrations.metaflow import DoltDT
downstream_doltdt = DoltDT(config=DoltConfig(database='path/to/hospital-price-analysis'))
run = downstream_doltdt.get_run('state_procedure_medians')
print(run.id)
```

We now have the run ID of flow that produced the medians form which we computed our variances. We can access the raw data, again directly from the integration:
```python
medians_doltdt = DoltDT(run=run.id)
df = medians_doltdt.read('prices')
print(df)
```

By storing Metaflow results in Dolt, result sets can be associated with flows and the input datasets. When results from Metaflow are put into other data stores we don't have a way to trace the table back to flow run that produced it. 

## Conclusion
In this post we demonstrated how to use Dolt alongside Metaflow. Metaflow provides a framework for defining data engineering and data science workflows. Using Dolt for inputs and outputs augments pipelines defined in Metaflow with additional capabilities. Users can examine a table in their Dolt database and locate the flow that produced that table, and if that flow used Dolt as an input, locate the flows that rate the input data, and so on. Users can also run a flow pinning a historical version of the data, providing for reproducible runs that use data version isolation to ensure code changes are properly tested. Finally, when Dolt is used as an input the commit graph can be used for back-testing against historical versions of the data.
