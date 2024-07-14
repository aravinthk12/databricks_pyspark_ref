from databricks_pyspark_ref.utils.helpers import get_spark_session, DataWorkflowBase


class SalesData(DataWorkflowBase):

    def _load_data(self, spark):

        self.raw_sales_data = (spark
                               .read
                               .format("csv")
                               .load("databricks_pyspark_ref/sample_datasets/Social_Network_Ads.csv"))

        self.raw_sales_data.show()
