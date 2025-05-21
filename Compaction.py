import logging
import sys
import boto3
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
logging.basicConfig(
   stream=sys.stdout,
   level=logging.INFO,
   format="%(levelname)s:%(module)s.%(name)s.%(funcName)s: %(message)s",
)
def is_empty_or_none(str_value):
   return str_value is None or str_value.strip() == ""
class S3Compactor:
   def __init__(self) -> None:
       self.logger = logging.getLogger(__class__.__name__)
       self.args = getResolvedOptions(
           sys.argv,
           [
               "JOB_NAME",
               "SOURCE_DB_NAME",
               "SOURCE_TABLE_NAME",
               "MAX_RECORDS_PER_FILE",
               "PUSH_DOWN_PREDICATE",
           ],
       )
       self.source_db_name = self.args["SOURCE_DB_NAME"]
       self.source_table_name = self.args["SOURCE_TABLE_NAME"]
       self.max_records_per_file = self.args["MAX_RECORDS_PER_FILE"]
       self.push_down_predicate = self.args["PUSH_DOWN_PREDICATE"]
       self.target_s3_folder = ""
       self.partition_by_col = []
       spark_session: SparkSession = SparkSession.builder.getOrCreate()
       self.glue_context: GlueContext = GlueContext(spark_session)
   def load_table_meta_info(self, database_name, table_name):
       glue_client = boto3.client("glue")
       response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
       self.target_s3_folder = response["Table"]["StorageDescriptor"]["Location"]
       self.partition_by_col = [
           col["Name"] for col in response["Table"]["PartitionKeys"]
       ]
   def run_compaction(self) -> None:
       """
       Executes the main data processing logic, which involves loading, transforming,
       and saving data to S3.
       """
       self.load_table_meta_info(self.source_db_name, self.source_table_name)
       if not self.partition_by_col:
           self.logger.error(
               f"Glue S3 Compaction job failed as {self.source_db_name}.{self.source_table_name} has no partitions."
           )
           sys.exit(1)
       if not self.target_s3_folder:
           self.logger.error(
               f"External location for {self.source_db_name}.{self.source_table_name} is empty."
           )
           sys.exit(1)
       if not is_empty_or_none(self.push_down_predicate):
           person_df = self.glue_context.create_dynamic_frame.from_catalog(
               database=self.source_db_name,
               table_name=self.source_table_name,
               transformation_ctx="load_dynamic_frame",
               push_down_predicate=self.push_down_predicate,
           )
       else:
           person_df = self.glue_context.create_dynamic_frame.from_catalog(
               database=self.source_db_name,
               table_name=self.source_table_name,
               transformation_ctx="load_dynamic_frame",
           )
       df_to_write = person_df.toDF()
       if df_to_write.isEmpty():
           self.logger.error("Empty Dataset cannot be compacted.")
           sys.exit(1)
       df_to_write.repartition(*self.partition_by_col).write.option(
           "compression", "snappy"
       ).mode("overwrite").format("parquet").partitionBy(
           *self.partition_by_col
       ).option(
           "mergeSchema", "true"
       ).option(
           "maxRecordsPerFile", self.max_records_per_file
       ).save(
           self.target_s3_folder
       )
       self.logger.error("Data written to Glue catalog table successfully.")
if __name__ == "__main__":
   S3Compactor().run_compaction()
