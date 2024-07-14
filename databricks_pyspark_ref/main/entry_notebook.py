from databricks_pyspark_ref.business_logics.SalesDataInsights import SalesDataInsights
from databricks_pyspark_ref.utils.helpers import get_spark_session
# from pyspark.dbutils import DBUtils
from datetime import datetime as dt

process_mapping = {
    "sales_agg_job": SalesDataInsights
}

if __name__ == "__main__":
    # creating spark instance
    spark = get_spark_session()

    # creating dbutils instance to work with databricks
    # new_dbutils = DBUtils(spark)

    # Initialize variables with default values
    process_name = "sales_agg_job"
    environment = "dev"
    process_date = dt.strftime(dt.now(), "%Y-%m-%d")  # Default to current date
    write_flag = "true"
    delta_load_flag = "true"

    # Retrieve values from Databricks workflows
    widget_names = ["process_name", "environment", "process_date", "market", "delta_load_flag"]
    for widget_name in widget_names:
        try:
            # globals()[widget_name] = new_dbutils.widgets.get(widget_name)
            # commenting out as not working with databricks
            pass
        except Exception as e:
            print(f"Failed to retrieve '{widget_name}' widget value: {str(e)}")

    # Calling the process to run
    process_mapping[process_name](environment=environment,
                                  process_date=process_date,
                                  write_flag=write_flag,
                                  delta_load_flag=delta_load_flag)
