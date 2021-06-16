from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pyspark.sql import Row


def filter_helper(csv_string): # TODO - move into file-specific module
    """
    Filter Lambda for RDD.
    Used to filter out all rows that do not match 
    expected row structure.

    :params cvs_string (string) - a row of a csv file as a string
        ex: '1974,5,19,1974.3795,333.37,5,-999.99,-999.99,50.40'
    
    :returns (boolean) - True or False
        True - data should be kept for downstream processing
        False - data should be dropped
    """
    header = 'year,month,day,decimal,average,ndays,1 year ago,10 years ago,increase since 1800' 
    comment_symbol = '#'
    return comment_symbol != csv_string[0] and csv_string != header


def structure_as_row(partitionData):
    """
    Map unstructured data (string) to structured representation
    with keys and values.

    :returns generator of Row records
    """
    header_keys = ['year', 'month', 'day', 'decimal', 'average', 'ndays', 'one_yr_ago', 'ten_yrs_ago', 'inc_since_1800'] # TODO - load from file-specific config
    for row in partitionData:
        values = row.split(',')
        if len(values) != len(header_keys):
            raise RuntimeError("Number of Header Keys Does Not Match Number of Field Values!")
        record = {}
        for idx, key in enumerate(header_keys):
            record[key] = values[idx]
        
        yield Row(**record)


def run_etl(ftp_path, spark=None):
    if not spark:
        spark = SparkSession.builder.getOrCreate()

    spark.sparkContext.addFile(ftp_path)
    data_path = SparkFiles.get(ftp_path.split('/')[-1])
    rdd = spark.sparkContext.textFile(data_path)
    # Use mapPartitions for structuring rows to only load
    # keys once per partition. Alternatively, we can consider
    # broadcasting the header_keys to workers...
    df = rdd.filter(filter_helper).mapPartitions(structure_as_row).toDF()
    df.write.parquet('sample_out')