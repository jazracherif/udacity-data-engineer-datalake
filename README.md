# Udacity Data Engineer Data Lake with Spark

[athena-tables]: https://github.com/jazracherif/udacity-data-engineer-datalake/blob/master/docs/athena-tables.png
[query-top-locations]: https://github.com/jazracherif/udacity-data-engineer-datalake/blob/master/docs/query-top-locations.png
[query-top-users]: https://github.com/jazracherif/udacity-data-engineer-datalake/blob/master/docs/query-top-users.png
[s3-buckets]: https://github.com/jazracherif/udacity-data-engineer-datalake/blob/master/docs/s3-buckets.png
[songplays-parquet]: https://github.com/jazracherif/udacity-data-engineer-datalake/blob/master/docs/songplays-parquet.png
[spark-etl-cluster]: https://github.com/jazracherif/udacity-data-engineer-datalake/blob/master/docs/spark-etl-cluster.png
[spark-executors]: https://github.com/jazracherif/udacity-data-engineer-datalake/blob/master/docs/spark-executors.png
[spark-jobs]: https://github.com/jazracherif/udacity-data-engineer-datalake/blob/master/docs/spark-jobs.png


In this project, I implement a datalake using S3 and AWS Elastic MapReduce (EMR).

Two datasets are used:
1. A song dataset, which is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/).
2. A log dataset, which consists of log files in JSON format generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above.

The files are stored in S3 bucket `s3a://udacity-dend` and are ingested into fact and dimension tables that are written as parquet file at location `s3://jazra-udacity-spark-etl/`

Fact Table:

    songplays - records in event data associated with song plays i.e. records with page NextSong
        songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables:

    users - users in the app
        user_id, first_name, last_name, gender, level
    songs - songs in music database
        song_id, title, artist_id, year, duration
    artists - artists in music database
        artist_id, name, location, lattitude, longitude
    time - timestamps of records in songplays broken down into specific units
        start_time, hour, day, week, month, year, weekday

The project consists of the following files:
- etl.py: A Spark python scrips that runs the extraction and transformation pipeline
- emr.py: A utility script to manage EMR ressources, create cluster, submit steps, and query for steps status.
- test_local.py: a script that extracts and prints the output of the ETL when it is run locally on the `data` files. 
- aws.cfg: the AWS credentials and region to use
- emr.cfg: The configuration parameters used to create an EMR cluster using the script emr.py


## Installation

Create the virtual environment: 

`python3 -m venv venv`

Activate the virtual environment: 

`source venv/bin/activate`

Install python packages: 

`pip3 install -r requirements.txt`

In all sections below, it is assume the venv environment is activated.


## Running the Pipeline in local mode

In order to run the pipeline locally, make sure to have a local install of spark:
`brew install apache-spark`

Run the ETL in spark standalone mode:

`spark-submit etl.py --mode local`

The output tables will be stored as parquet files in a new folder called `out`.

to view a sample dataset from the generated tables, run:

`python test_local.py`


##  Running The Pipeline in EMR

To create the EMR cluster and run one instance of the etl.py pipeline, run: 

`python emr.py --cmd create-cluster`

This command will create the cluster, find the hostname for the master node, scp the etl.py file to the master node, and initiate an EMR step that runs the Pipeline from within the master node.

to check the status of the clusters, run:

`python emr.py --cmd describe-clusters`

you can also login into AWS EMR console and verify the cluster is in the RUNNING or WAITING mode:

![emr console][spark-etl-cluster]

You can also verify that the ETL pipeline is running by listing the steps in the cluster:

`python emr.py --cmd list_clusters_steps`

The job should also appear in the Spark History Server UI:

![spark-jobs][spark-jobs]

We can also look at the list of the Spark executors:

![spark executors][spark-executors]

### Output

The final tables will be stored as folders in S3 buckets defined in etl.py.

![s3 buckets][s3-buckets]

The files format is parquet, using snappy compression. Here is an example of the songplays files:

![songplays parquet][songplays-parquet]

## Analysis with Amazon Athena

After running the pipeline, we can do some analysis by ingesting the S3 data using [Amazon Athena](https://aws.amazon.com/athena/)

First, setup a Glue crawler that points to the bucket folder where the generated tables files are stored. The crawler will automatically extract the tables from the parquet files.

After a few minutes, tables will show up in the Athena console under `Query Editor`:

![Athena tables][athena-tables]

We can run a query to generate the top 5 most active listeners on the platform using the following query:

~~~ sql
WITH top_users AS (
    SELECT user_id, COUNT(*) AS count
    FROM songplay
    GROUP BY user_id
    ORDER BY cnt DESC
    LIMIT 5
)
SELECT users.first_name, 
       users.last_name, 
       top_users.cnt
  FROM top_users
 INNER JOIN users
       ON users.user_id = top_users.user_id
 ORDER BY cnt DESC
~~~~

and we get:

| first_name | last_name | cnt |
| ------------- |:-------------:|:-------------:|
| Chloe | Cuevas | 689 |
| Tegan | Levine | 665 |
| Kate | Harrell | 557 |
| Lily | Koch | 463 | 
| Aleena | Kirby | 397 |

See the results in Athena:

![query-top-users][query-top-users]


We can also look for the top 5 most popular locations where songs are played, using the following query:

~~~ sql
SELECT location, 
       count(*) AS cnt 
  FROM songplay
 GROUP BY location 
 ORDER BY cnt DESC 
 LIMIT 5
~~~~

| location | count |
| ------------- |:-------------:|
| San Francisco-Oakland-Hayward, CA | 691
| Portland-South Portland, ME | 665
| Lansing-East Lansing, MI | 557
| Chicago-Naperville-Elgin, IL-IN-WI | 475
| Atlanta-Sandy Springs-Roswell, GA | 456

See the results in Athena:

![query top locations][query-top-locations]
