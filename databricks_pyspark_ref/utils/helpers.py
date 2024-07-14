from pyspark.sql import SparkSession
from abc import ABC, abstractmethod


def get_spark_session():
    """
    Create and return a SparkSession with the application name 'spark_entry'.

    This function initializes a SparkSession if it does not already exist,
    and returns the existing SparkSession if it has already been created.
    The application name for the Spark session is set to 'spark_entry'.

    Returns:
        SparkSession: An active SparkSession with the application name 'spark_entry'.
    """
    return SparkSession.builder.appName("spark_entry").getOrCreate()


class DataWorkflowBase(ABC):
    def __init__(
            self,
            environment: str,
            process_date: str,
            write_flag: str,
            delta_load_flag: str = 'true'

    ):
        self.environment = environment
        self.process_date = process_date
        self.delta_load_flag = delta_load_flag

        # main run
        self._load_data(get_spark_session())
        self._process_data()
        if write_flag == "true":
            self._populate_tables()
        else:
            print("process ran successfully, but write_flag is set to false")

    # @abstractmethod
    def _load_data(self, spark):
        pass

    # @abstractmethod
    def _process_data(self):
        pass

    # @abstractmethod
    def _populate_tables(self):
        pass

    @staticmethod
    def _read_data(table_name):
        """
        Reader method to read data from a specified table name.
        This method is intended to be used by subclasses.
        """
        # Implement your data reading logic here
        spark = get_spark_session()
        return spark.table(table_name)

    @staticmethod
    def _populate_table():
        pass
