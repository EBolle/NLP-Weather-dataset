# Udacity review

This page explains my thought process under 3 different scenarios, the outline of the steps taken in this project and
the data model can be found in the README or the data_model.MD file. 

**The data was increased by 100x**

With my current experience I think of this project as 2 main 'data' steps:

- storage of the data so it can be processed and afterwards used for analysis
- processing the data

Simply put, the current setup would not change much if the data was increased by 100x. The main reason for this is that
the current setup leverages the (AWS) cloud to store and process the data. 

- storage for processing: S3 on AWS is highly scalable storage wise and performance wise. A single object in S3 can be up to 5TB so 
increasing the data 100x times is still a fraction of what is possible in S3. Dependent on te compute cluster S3 can 
handle up to 3.500 requests per second to add data and up to 5.500 requests per second to retrieve data.
This project currently handles approximately 5GB in compressed format, handling 500GB would still be very easy to handle
on S3 ([source][s3]).

- processing: for this step Spark on EMR is used. From their [website][spark] you can read that Spark can handle up to
petabytes of data and that many organizations use thousands of clusters to handle their data processing jobs. These 
clusters can be flexibly handled by EMR. Therefore, to process 500GB of data I would stick to Spark on EMR. However, 
dependent on performance I would consider using more nodes and/or choosing bigger nodes with better performance.

**The pipelines would be run on a daily basis by 7 am every day**

Since the purpose of this pipeline is to provide historical data for NLP research, it does not make sense to run it on
a daily basis. However, if we pretend that the Yelp and GHCN data was updated on a daily basis, I would take the following
steps:

- Write a Python script that either retrieves the daily data via their API's, or downloads their newest data via a curl
like command
- Dependent on the amount of time it takes to download and process the data, this script should start running a decent
amount of time before 7 am
- The execution of this script, together with the `local_utils` python scripts and the `spark_app` scripts, should all 
be handled with Airflow via 1 or more DAGS, including data quality checks and email / slack messages when the pipeline
breaks.

**The database needed to be accessed by 100+ people.**

The 'database' of this project is S3. S3 is used by some of the biggest organizations in the world, handling far more
than 100+ people. Just like handling 100x the size of the data, the (AWS) cloud provides a flexible, highly scalable
solution for which 500GB of data or 100+ people is not that difficult. 


[s3]: https://aws.amazon.com/s3/features/
[spark]: https://spark.apache.org/faq.html
