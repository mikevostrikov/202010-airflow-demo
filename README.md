# Apache Airflow for Data Warehouse Demo

## Background

These days Apache Airflow is a popular tool for ETL workflows
orchestration.

I have designed and implemented ETL workflows orchestration previously
using other technologies. On this project, I am exploring how it would
look like with Apache Airflow.

I came up with a set of orchestration-related requirements typical in
Data Warehousing according to my experience. To meet the requirements,
I designed and implemented several Airflow DAGs and their 
relationships.

I focus solely on the pipelines orchestration on this project, and I
omit all other parts of the solution (like data models).

## Requirements

An organization XYZ is building a data warehouse with a three-layer
architecture:
* Landing Zone (LZ) - stores daily snapshots of raw incoming data
* Data Warehouse Core (DWH) - stores integrated data from all the
source systems in a common data model
* Data Marts (DM) - store data from the DWH in a report-specific
format

The data should be loaded into the data warehouse from the two source
systems:
* Alpha System - back-office accounting system
  * The system provides `Accounts` data
     * There is no historical data in the system, only the current
     state of all `Accounts` is available 
  * The system provides `Balances` data
    * There are two years of historical data available in the system
    * For every day for every `Account`, there is a `Balance Amount`
    * `Balance Amount` can be adjusted in the system during the next
    three days after the `Balance date` (due to accounting of
    backdating documents)
      * These adjustments should be loaded into the data warehouse core
      automatically.
      * Usually, not more than 0.01% of all balances require adjustments
* Beta System - front-office products system
  * The system provides `Deals` data
    * There is no historical data in the system, only the current state
    of all `Deals` is available 
    * The deployment consists of two instances of the system:
    `South` and `North` - serving two regions of the country

The finance department requires two reports to be generated based on the
all mentioned data:
* `Daily Report` - should be available the next day
* `Weekly Report` - should be available every Thursday in the morning

## Design

#### Solution structure

Typically the data warehouse solution is split into high-level modules
by layers, source systems, and subject areas. Applying this approach to
the whole data warehouse dataflow, we can identify a few logical
high-level DAGs:
1. Alpha System loading to Landing Zone
1. Beta System loading to Landing Zone
1. Alpha System loading to Data Warehouse Core
1. Beta System loading to Data Warehouse Core
1. Finance Data Mart loading

<p align="center">
  <img width="460" height="300" src="https://raw.githubusercontent.com/mikevostrikov/202010-airflow-demo/master/doc/data-warehouse.svg">
</p>

Each high-level DAG can be further split into more DAGs as needed.
Every DAG can be developed and maintained in different projects
by multiple teams.

The overall code structure for the data pipelines under Airflow
management is designed to reflect the data warehouse's logical
structure and the identified high-level DAGs. This code
structure can be viewed in this git repo.

#### Cross-DAG dependencies & DAG interfaces

Every DAG is responsible for its part of the data warehouse dataflow,
meaning that there are upstream and downstream DAGs and
cross-DAGs dependencies between them.

Dependencies between projects and teams should always be taken special
care. Particularly, maintainers of a DAG should understand what parts
of their DAG are internal and can be changed without any impact on
other DAGs and what parts are public. These public parts of a DAG
comprise its interface and, and once in PROD, they cannot be changed
easily because other DAGs may already depend on them.

To ease cross-DAGs dependencies management, we introduce DAG
interfaces. Every DAG is implemented as a Python module named
`<dag_id>.py`, while its interface is a separate Python module named
`<dag_id>_public.py`. The interface contains the `DAG_ID` of the DAG
and an enumeration of `TASK_ID`s that other DAGs can depend on (e.g.
sensed with `ExternalTaskSensor`).

#### Instances of the Beta System

The list of instances can be configured in
`/dags/commons/configs/beta_instances.py`. For every instance
a separate DAG is created dynamically. There are two similar DAGs
for loading Beta System data to Landing Zone: one for `North` and one
for `South` instance. Similarly, two instances are created for loading
Beta System data to Data Warehouse Core.

Some Beta System data consumers may not differentiate between
the system's instances. For example, they may be interested only in the
fact that all deals from all the instances of the system are loaded. For
these consumers an auxiliary `dwh_beta_all` DAG is implemented.
This DAG senses the readiness of a specific task in every system's
instance DAG. When all these tasks are ready, the respective task in
`dwh_beta_all` is marked as `success`.

#### Incremental load of previous dates balance adjustments

`Account Balances` is one of the largest entities in
the data warehouse. It would be a very time-consuming and 
resource-intensive task to perform a full reload of the entity in order
to reflect balance adjustments to previous dates. Incremental load is
the right tool for the task.

Unfortunately, the Beta System does not provide data required for
selecting the recent changes in the data. That is why we design an 
ETL-step to compare the balances snapshot from the Landing Zone with
the current snapshot for the same date in the source system. The
detected delta is then stored in a `delta log` in the Landing Zone.
After that, another ETL-step applies this delta to the balances 
snapshots in Landing Zone that ensures that they are in sync with the
source system.

Downstream ETL-pipelines should use the `delta log` if they have to
reflect the adjustments. 

The whole data flow that populates `Account balances` consists of the
full load of the execution date data and the incremental load of the
previous dates' adjustments. This approach is somewhat close
to the lambda architecture.

<p align="center">
  <img width="250" height="200" src="https://raw.githubusercontent.com/mikevostrikov/202010-airflow-demo/master/doc/incremental-load-lambda.svg"/>
</p>

## Screenshots
##### All DAGs
<p align="center">
  <img src="https://raw.githubusercontent.com/mikevostrikov/202010-airflow-demo/master/doc/screenshot-all-dags.png"/>
</p>

##### DAG lz_alpha
<p align="center">
  <img src="https://raw.githubusercontent.com/mikevostrikov/202010-airflow-demo/master/doc/screenshot-lz-alpha.png"/>
</p>

##### SubDAG lz_alpha.balances_increment
<p align="center">
  <img src="https://raw.githubusercontent.com/mikevostrikov/202010-airflow-demo/master/doc/screenshot-lz-alpha-balances-increment.png"/>
</p>

##### DAG lz_beta.north
<p align="center">
  <img src="https://raw.githubusercontent.com/mikevostrikov/202010-airflow-demo/master/doc/screenshot-lz-beta-north.png"/>
</p>

##### DAG dwh_alpha
<p align="center">
  <img src="https://raw.githubusercontent.com/mikevostrikov/202010-airflow-demo/master/doc/screenshot-dwh-alpha.png"/>
</p>

##### DAG dwh_beta.north
<p align="center">
  <img src="https://raw.githubusercontent.com/mikevostrikov/202010-airflow-demo/master/doc/screenshot-dwh-beta-north.png"/>
</p>

##### DAG dwh_beta_all
<p align="center">
  <img src="https://raw.githubusercontent.com/mikevostrikov/202010-airflow-demo/master/doc/screenshot-dwh-beta-all.png"/>
</p>

##### DAG dm_finance
<p align="center">
  <img src="https://raw.githubusercontent.com/mikevostrikov/202010-airflow-demo/master/doc/screenshot-dm-finance.png"/>
</p>
