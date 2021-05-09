"""This module lets you re-create the dataflow diagram from the README."""


from diagrams import Cluster, Diagram
from diagrams.aws.analytics import EMRCluster
from diagrams.aws.storage import SimpleStorageServiceS3Bucket
from diagrams.onprem.analytics import Spark
from diagrams.programming.flowchart import Document, MultipleDocuments
from diagrams.programming.language import Python


with Diagram("Dataflow NLP-Weather-dataset", show=True):
    with Cluster('Local input'):
        local_input = [MultipleDocuments('Yelp files'), MultipleDocuments('GHCN files')]

    with Cluster('AWS'):
        s3_bucket_input = SimpleStorageServiceS3Bucket('<your_s3_bucket>')
        s3_bucket_output = SimpleStorageServiceS3Bucket('<your_s3_bucket>')
        emr = EMRCluster('EMR')
        spark = Spark('spark_app')

    with Cluster('Local output'):
        local_output = Document('nlp_weather_dataset')

    local_input >> Python('local_to_s3') >> s3_bucket_input
    s3_bucket_input >> emr >> spark >> s3_bucket_output
    s3_bucket_output >> Python('s3_to_local') >> local_output
