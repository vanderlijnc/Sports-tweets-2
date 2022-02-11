import psycopg2
import pandas as pd

def get_data(limit = None):
    """
    Retrieves Twitter data from server from table X from schema Y and saves it as csv chunks of 500 000 in root folder.
    The function is meant to process all tweets (38,5 million), so it takes a while. Limit can be used to test.
    Parameters to access the database are anonymized.
    
    Parameters:
    
    limit: optional, limits the number of records retrieved. 
    """
    
    # set up database connection
    con = psycopg2.connect(database="some_new", user="ehem", password="DigiGeoEmil!",
                       host="dgl-data.geography.helsinki.fi")
    
    if limit == None:
        query1 = 'SELECT lang, full_text, geom, lat, lon FROM twitter_histories.twitter_histories_full_fin_est_fulltext;'
        
    else:
        query1 = 'SELECT lang, full_text, geom, lat, lon FROM twitter_histories.twitter_histories_full_fin_est_fulltext LIMIT ' + str(limit) + ';'
    
    batch_no=1
    
    for chunk in pd.read_sql(query1, con, chunksize = 500000):
        chunk.to_csv('data/chunk'+str(batch_no)+'.csv',index=False)
        
        print('Chunk ' + str(batch_no) + ' is done')
        batch_no+=1


    return

get_data()