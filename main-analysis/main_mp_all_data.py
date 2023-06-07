# Import required packages
import os
import time
import glob
import pandas as pd
import spacy
import spacy_stanza
import stanza
import matplotlib.pyplot as plt
import geopandas as gpd
import requests
import geojson
from shapely.geometry import Point
from pyproj import CRS

# Get starting time
script_start = time.time()

# Define functions
def create_pipeline(lang, package_name):
    """Creates a stanza pipeline for the language given as argument.

    Parameters:

    lang| String: abbreviation for language which pipeline you
    want to create eg. 'fi' or 'en'.
    """
    # To avoid an error
    os.environ['KMP_DUPLICATE_LIB_OK']='TRUE'
    # Make the pipeline for tokenizing and lemmatization
    nlp_pipeline = stanza.Pipeline(lang, processors='tokenize, lemma, pos', package=package_name)
    nlp = spacy_stanza.StanzaLanguage(nlp_pipeline)
    print('Stanza pipeline created for language: ' + lang)
    return nlp

def create_lemmas_lambda(df, nlp_lang):
    """ Lemmatizes a pandas column based on a NLP pipeline.
    """
    # Create a df copy so pandas don't give a warning
    df2 = df.copy()
    # Get start time
    start_time = time.time()

    # Replace hashtags with empty string
    df2['full_text'] = df2['full_text'].replace('#', '')
    # Create a doc for each row in the full_text column
    df2['lemma_text_doc'] = df2.apply(lambda row: nlp_lang(row['full_text']), axis=1)
    # Create a list of lemmas into column
    df2['lemmas'] = df2.apply(lambda row: [token.lemma_ for token in row['lemma_text_doc']], axis=1)
    # Stitch together list of lemmas into other column
    df2['lemma_text'] = df2.apply(lambda row: ' '.join(row['lemmas']), axis=1)
    # Drop unnessecary column
    df2 = df2.drop(columns=['lemma_text_doc'])
    # Print the time it took to lemmatise x amount of tweets
    tweet_count = len(df2)
    total_time = round((time.time() - start_time)/60, 3)

    print('--- Lemmatising %s tweets took %s minutes ---' % (tweet_count, total_time))
    return df2

def get_sports_tweets(df, keyword_list):
    """
    Searches for matches to sports-related keywords from a dataframe that has been lemmatised.

    Parameters:

    df | String: name of Pandas dataframe with lemmatized tweets
    keyword_list | list of strings: list of sports-related words to search from the tweets
    """

    sports_df = pd.DataFrame()

    for i, row in df.iterrows():

        try:
            # Check if keywords are found in lemmas
            count = [lemma for lemma in df['lemmas'][i] if(lemma in keyword_list)]

            # Append the matched rows to sports dataframe
            if len(count) > 0:
                sports_df = sports_df.append(row)

        except TypeError:
            print('Encountered a TypeError')

    print(str(len(sports_df)) + ' tweets found with sports related keywords')
    return sports_df

def geocode(sportstogeocode, hmanames):
    """
    Geocodes the tweets in sportstogeocode dataframe based on the gazetteer saved in hmanames dataframe.

    Parameters:

    sportstogeocode | String: name of Pandas dataframe with ungeotagged tweets
    hmanames | String: name of GeoPandas dataframe holding gazetteer information
    """
    # Create a list of the placenames for comparison purposes
    placenames = [i.lower() for i in list(hmanames['name'])]

    # Create a new geodataframe which will hold the geocoded tweets
    sportshma = gpd.GeoDataFrame()

    for i, row in sportstogeocode.iterrows():

        try:
            # Search if any lemmas match the list of placenames
            placelist = [lemma.lower() for lemma in sportstogeocode.loc[i, 'lemmas'] if(lemma.lower() in placenames)]

            # If there are any placenames, retrieve the point for that place and add it to the tweet information
            if len(placelist) > 0:
                x = hmanames.loc[hmanames['name']== placelist[0], 'geometry'].values[0].x
                y = hmanames.loc[hmanames['name']== placelist[0], 'geometry'].values[0].y
                geom = hmanames.loc[hmanames['name']== placelist[0], 'geometry'].values[0]
                row['geometry'] = geom
                row['lon'] = x
                row['lat'] = y
                sportshma = sportshma.append(row)

        except TypeError:
            print('Encountered a TypeError')

    print(str(len(sportshma)) + ' tweets succesfully geocoded')
    return sportshma

def parse_points(sportsgeotagged_o):
    """
    Takes geotagged tweets and parses the coordinates into points. Returns a geodataframe with the tweets which are geotagged inside Helsinki Metropolitan area.

    Parameters:

    sportsgeotagged | String: name of Pandas dataframe with geotagged tweets
    """
    sportsgeotagged = sportsgeotagged_o.copy()
    #parse shapely points from coordinates and convert to epsg 3067
    sportsgeotagged['geometry'] = None


    geom = []
    for x, y in zip(sportsgeotagged['lon'], sportsgeotagged['lat']):
        try:
            geom.append(Point(x, y))

        except (ValueError, KeyError):
            geom.append(None)
            print('Value Error or Key Error while parsing points')

    sportsgeotagged['geometry'] = geom
    gdf = gpd.GeoDataFrame(sportsgeotagged, geometry='geometry')
    gdf.crs = CRS.from_epsg(4326).to_wkt()
    gdf = gdf.to_crs(epsg=3067)

     # Fetch city border data from WFS using requests
    r = requests.get('http://geo.stat.fi/geoserver/tilastointialueet/wfs', params=dict(service='WFS', version='2.0.0', request='GetFeature', typeName='tilastointialueet:kunta1000k', outputFormat='json'))

    # get a shapefile of municipalities
    municip = gpd.GeoDataFrame.from_features(geojson.loads(r.content),  crs='EPSG:3067')
    # only keep HMA area
    hma = municip.loc[municip['nimi'] == ['Vantaa', 'Espoo', 'Helsinki', 'Kauniainen']]
    hma = hma.to_crs(epsg=3067)
    hma.drop(['kunta', 'vuosi', 'nimi', 'name', 'namn', 'bbox'], axis=1, inplace=True)
    hmageotagged = gpd.overlay(gdf, hma, how='intersection')

    return hmageotagged


if __name__ == '__main__':

    # Download language models
    stanza.download(lang='en')
    stanza.download(lang='fi')
   

    # Create pipelines
    nlp_en = create_pipeline('en', 'ewt')
    nlp_fi = create_pipeline('fi', 'tdt')


    # Create a geodataframe for final output
    final_df = gpd.GeoDataFrame()

    # Read all tweets
    df = pd.read_pickle('all_data.pkl')
    df = df[0:10000]

    # Separate Finnish, English and Estonian to separate dataframes
    df_fi = df[df['lang']=='fi']
    df_en = df[df['lang']=='en']


    # Lemmatize tweet text, using correct language pipeline
    results_fi = create_lemmas_lambda(df_fi, nlp_fi)
    results_en = create_lemmas_lambda(df_en, nlp_en)


    # Retrieve sports related tweets based on keyword lists
    sportslist_fi = ['kävely', 'kävellä', 'käveleminen' 'juoksu', 'juosta', 'juokseminen', 
                 'lenkki', 'lenkkeily', 'lenkkeillä', 'treenata', 'patikointi', 'patikoida', 'patikoiminen',
                  'pyörä', 'pyöräily', 'pyöräillä', 'pyöräileminen']

    sports_fi = get_sports_tweets(results_fi, sportslist_fi)

    sportslist_en = ['running', 'run', 'walk', 'walking', 'jog' ,'jogging', 'hike', 'hiking', 'trek', 'trekking',
                  'bicycle', 'bike', 'biking','cycling']


    sports_en = get_sports_tweets(results_en, sportslist_en)





    # Combine dataframes of sports tweets
    sports = sports_en.append(sports_fi)


    # Figure out which sports tweets already have geotags
    sportstogeocode = sports[sports['geom'].isna()]
    sportsgeotagged = sports[~(sports['geom'].isna())]

    # Read in gazetteer
    hmanames = gpd.read_file(r'hma_gazetteer.gpkg')
    # Geocode the tweets without geolocation
    sportshma = geocode(sportstogeocode, hmanames)

    # Parse points from geotagged tweets
    sportsgeotagged_hma = parse_points(sportsgeotagged)

    # Combine back to the geocoded tweets
    if len(sportshma) > 0:
        sportshma_combined = sportshma.append(sportsgeotagged_hma)
        final_df = final_df.append(sportshma_combined)
    else:
        final_df = final_df.append(sportsgeotagged_hma)

    # Drop columns of lists
    final_df = final_df.drop(['lemmas'], axis=1)
    final_df = final_df.drop(['geom'], axis=1)

    if len(final_df) > 0:

        final_df.to_file('finaloutput_lambda_10000.gpkg', driver='GPKG')
        print('---Geopackage created---')
    else:
        print('--- Final dataframe is empty ---')

    print('Time it took in minutes: ')
    print((time.time()-script_start)/60)
