from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, flatten, when, lit, expr, array
from pyspark.sql.types import ArrayType, StructType

class DataReader:
    """
    A class to handle reading data from various file formats in a Spark environment.

    Attributes
    ----------
    supported_extensions : tuple
        A tuple containing the file extensions supported by the DataReader ('csv', 'dsv', 'parquet', 'excel').

    Methods
    -------
    is_supported_extension(extension):
        Checks if the provided file extension is supported.

    data_is_available(source_folder_path):
        Checks if data is available in the specified folder. Returns True if data is available, otherwise False.

    read_data(source_file_path, df_meta):
        Determines the appropriate method to read data based on the file extension and returns the DataFrame.
        Logs an error and exits in case of an unsupported file extension or other errors.

    _read_parquet(source_file_path):
        Reads data from a Parquet file and returns a Spark DataFrame.

    _read_excel(source_file_path):
        Reads data from an Excel file, converts it to a Spark DataFrame, and returns it.

    _read_csv_or_dsv(source_file_path, df_meta):
        Reads data from a CSV or DSV file and returns a Spark DataFrame.

    _read_json(source_file_path, df_meta):
        Reads data from a JSON file, and if requested, flattens the structure, returning a Spark DataFrame.

    _flatten_df(nested_df):
        Recursively flattens any nested structures (structs and arrays) in a DataFrame.
    """

    def __init__(self):
        """
        Constructs all the necessary attributes for the DataReader object.
        Initializes the supported file extensions.
        """
        self.supported_extensions = ("csv", "dsv", "parquet", "xlsx", "json")


    def is_supported_extension(self, extension):
        """
        Checks if the provided file extension is supported.

        Parameters
        ----------
        extension : str
            The file extension/type to check.

        Returns
        -------
        bool
            True if the extension is supported, False otherwise.
        """
        return extension in self.supported_extensions


    def data_is_available(self, source_folder_path):
        """
        Checks if data is available in the specified folder.

        Parameters
        ----------
        source_folder_path : str
            Path to the folder where the source files are stored.

        Returns
        -------

        bool
            True if data is available in the source folder, False otherwise.

        Raises
        ------
        SystemExit
            If no data is available in the source folder, the method will log and trigger a notebook exit.
        """
        files = mssparkutils.fs.ls(source_folder_path)
        return len(files) > 0


    def read_data(self, source_file_path, df_meta):
        """
        Determines the appropriate method to read data based on the file extension and returns the DataFrame.

        Parameters
        ----------
        source_file_path : str
            Path to the specific source file(s).
        df_meta : dict
            Metadata containing information about the source data, including file type and options.

        Returns
        -------
        DataFrame
            A Spark DataFrame containing the data from the source file.

        Raises
        ------
        SystemExit
            If the file extension is unsupported or an error occurs during data reading, the method will log and exit the notebook.
        """
        extension = df_meta["DestinationExtension"]

        if extension == "parquet":
            return self._read_parquet(source_file_path)

        elif extension == "xlsx":
            return self._read_excel(source_file_path)

        elif extension in ["csv", "dsv"]:
            return self._read_csv_or_dsv(source_file_path, df_meta)

        elif extension == "json":
            return self._read_json(source_file_path, df_meta)


    def _read_parquet(self, source_file_path):
        """
        Reads data from a Parquet file and returns a Spark DataFrame.

        Parameters
        ----------
        source_file_path : str
            Path to the specific Parquet file.

        Returns
        -------
        DataFrame
            A Spark DataFrame containing the data from the Parquet file.
        """
        return spark.read \
                    .format("parquet") \
                    .option("inferSchema", "true") \
                    .load(source_file_path)


    def _read_excel(self, source_file_path):
        """
        Reads data from an Excel file, converts it to a Spark DataFrame, and returns it.

        Parameters
        ----------
        source_file_path : str
            Path to the specific Excel file.

        Returns
        -------
        DataFrame
            A Spark DataFrame containing the data from the Excel file.
        """
        df_source = pd.read_excel(source_file_path, dtype=str)
        return spark.createDataFrame(df_source)


    def _read_csv_or_dsv(self, source_file_path, df_meta):
        """
        Reads data from a CSV or DSV file and returns a Spark DataFrame.

        Parameters
        ----------
        source_file_path : str
            Path to the specific CSV or DSV file.
        df_meta : dict
            Metadata containing options such as delimiter, encoding, and other CSV/DSV-specific settings.

        Returns
        -------
        DataFrame
            A Spark DataFrame containing the data from the CSV or DSV file.
        """
        return spark.read \
                    .format("csv") \
                    .option("header", df_meta["FirstRowHasHeader"]) \
                    .option("delimiter", df_meta["ColumnDelimiter"]) \
                    .option("encoding", df_meta["Encoding"]) \
                    .option("quote", df_meta["QuoteCharacter"]) \
                    .option("escape", df_meta["EscapeCharacter"]) \
                    .option("inferSchema", "true") \
                    .option("delta.columnMapping.mode", "name") \
                    .load(source_file_path)


    def _read_json(self, source_file_path, df_meta):
        """
        Reads data from a JSON file and optionally flattens any nested structures.

        Parameters
        ----------
        source_file_path : str
            Path to the specific JSON file.
        df_meta : dict
            Metadata containing information about the source data, including flattening options.

        Returns
        -------
        DataFrame
            A Spark DataFrame containing the data from the JSON file.
        """
        df_source = spark.read\
                         .format("json")\
                         .option("multiline", "true")\
                         .load(source_file_path)

        df_source = self._flatten_df(df_source)

        return df_source
            

    def _flatten_df(self, nested_df: DataFrame) -> DataFrame:
        """
        Recursively flattens any nested structures (structs and arrays) in a DataFrame.

        Parameters
        ----------
        nested_df : DataFrame
            A Spark DataFrame containing potentially nested structures (e.g., structs, arrays).

        Returns
        -------
        DataFrame
            A Spark DataFrame with all nested structures expanded into individual columns.
        """
        while True:
            # Check if there are any StructType or ArrayType columns
            complex_fields = [(field.name, field.dataType) for field in nested_df.schema.fields if isinstance(field.dataType, (StructType, ArrayType))]

            if not complex_fields:
                break

            for name, dtype in complex_fields:
                if isinstance(dtype, StructType):
                    # If it's a struct, expand it into its individual fields
                    expanded = [col(f"{name}.{field.name}").alias(f"{name}_{field.name}") for field in dtype]
                    nested_df = nested_df.select("*", *expanded).drop(name)

                elif isinstance(dtype, ArrayType):
                    # Track empty arrays by replacing them with null if empty, then expanding
                    nested_df = nested_df.withColumn(name, when(expr(f"size({name}) > 0"), col(name)).otherwise(array(lit(None))))
                    
                    # Now use explode to handle non-empty arrays
                    nested_df = nested_df.withColumn(name, explode(col(name)))

        return nested_df
