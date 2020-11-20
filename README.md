
# Music data analyzer scheduler 

This project showcases how to design and schedule a series of jobs/steps using [Apache Airflow](https://airflow.apache.org/) with the following purposes
- Backfill data
- Build a dimensional data model using python
- load data from AWS S3 bucket to AWS Redshift Datawarehouse
- run quality checks on the data
- Use or create custom operators and available hooks to create reusable code

## Running the DAG

You can run the DAG on your own machine using `docker-compose`. To use `docker-compose`, you
must first [install Docker](https://docs.docker.com/install/). Once Docker is installed:

1. Open a terminal in the same directory as [`docker-compose.yml`](docker-compose.yml) an
1. Run `docker-compose up`
1. Wait 30-60 seconds
1. Open [`http://localhost:8080`](http://localhost:8080) in Google Chrome (Other browsers occasionally have issues rendering the Airflow UI)
1. Make sure you have configured the `aws_credentials` and `redshift` connections in the Airflow UI

When you are ready to quit Airflow, hit `ctrl+c` in the terminal where `docker-compose` is running.
Then, type `docker-compose down`

![img](dag.png)