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