from databricks_pyspark_ref.utils.helpers import DataWorkflowBase
import pyspark.sql.functions as F


class SalesDataInsights(DataWorkflowBase):
    """
    A class used to generate insights from sales data.

    Methods
    -------
    _load_data(spark)
        Loads the raw sales data from a CSV file into a Spark DataFrame.

    _gender_purchase_details()
        Aggregates purchase data by gender and calculates the percentage of total purchases for each gender.

    _process_data()
        Processes the data to generate the required insights.

    _populate_tables()
        Exports the processed gender purchase details to a CSV file.
    """

    def _load_data(self, spark):
        """
        Loads the raw sales data from a CSV file into a Spark DataFrame.

        Parameters
        ----------
        spark : SparkSession
            The Spark session used to load the data.
        """
        self.raw_sales_data = (spark
                               .read
                               .format("csv")
                               .options(**{"header": "true"})
                               .load("databricks_pyspark_ref/sample_datasets/raw_sales_data.csv"))

    def _gender_purchase_details(self):
        """
        Aggregates purchase data by gender and calculates the percentage of total purchases for each gender.
        """
        total_sales = self.raw_sales_data.agg(F.sum("Purchased").alias("total_sales")).collect()[0]["total_sales"]

        self.gender_purchase_df = (
            self.raw_sales_data
            .groupBy("Gender")
            .agg(F.sum("Purchased").alias("purchase_count"))
            .withColumn("purchase_percentage", F.round(F.col("purchase_count") / total_sales * 100, 2)))

    def _process_data(self):
        """
        Processes the data to generate the required insights.
        """
        self._gender_purchase_details()

    def _populate_tables(self):
        """
        Exports the processed gender purchase details to a CSV file.
        """
        (self.gender_purchase_df
         .toPandas()
         .to_csv("databricks_pyspark_ref/output_datasets/gender_purchase_details.csv", index=False))


