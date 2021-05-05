def get_data(limit = None):
    """
    Retrieves Twitter data from server from table X from schema Y and saves it as csv chunks of 500 000 in root folder.
    The function is meant to process all tweets (38,5 million), so it takes a while. Limit can be used to test.
    Parameters to access the database are anonymized.
    
    Parameters:
    
    limit: optional, limits the number of records retrieved. 
    """

    # set up database connection
    con = psycopg2.connect(database="db", user="username", password="m32!B?daWo",
                       host="host@host.fi")
    
    if limit == None:
        sql = 'SELECT lang, full_text, geom, lat, lon FROM schema.table;'
        
    else:
        sql = 'SELECT lang, full_text, geom, lat, lon FROM schema.table LIMIT ' + str(limit) + ';'
    
    batch_no=1
    
    for chunk in pd.read_sql(sql, con, chunksize = 500000):
        chunk.to_csv('chunk'+str(batch_no)+'.csv',index=False)
        batch_no+=1
    
    return

get_data()