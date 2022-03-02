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

def geocode(sportstogeocode, jyvnames):
    """
    Geocodes the tweets in sportstogeocode dataframe based on the gazetteer saved in jyvnames dataframe.

    Parameters:

    sportstogeocode | String: name of Pandas dataframe with ungeotagged tweets
    jyvnames | String: name of GeoPandas dataframe holding gazetteer information
    """
    # Create a list of the placenames for comparison purposes
    placenames = [i.lower() for i in list(jyvnames['name'])]

    # Create a new geodataframe which will hold the geocoded tweets
    sportsjyv = gpd.GeoDataFrame()

    for i, row in sportstogeocode.iterrows():

        try:
            # Search if any lemmas match the list of placenames
            placelist = [lemma.lower() for lemma in sportstogeocode.loc[i, 'lemmas'] if(lemma.lower() in placenames)]

            # If there are any placenames, retrieve the point for that place and add it to the tweet information
            if len(placelist) > 0:
                x = jyvnames.loc[jyvnames['name']== placelist[0], 'geometry'].values[0].x
                y = jyvnames.loc[jyvnames['name']== placelist[0], 'geometry'].values[0].y
                geom = jyvnames.loc[jyvnames['name']== placelist[0], 'geometry'].values[0]
                row['geometry'] = geom
                row['lon'] = x
                row['lat'] = y
                sportsjyv = sportsjyv.append(row)

        except TypeError:
            print('Encountered a TypeError')

    print(str(len(sportsjyv)) + ' tweets succesfully geocoded')
    return sportsjyv

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
    # only keep Jyväskylä area
    jyv = municip.loc[municip['nimi'] == 'Jyväskylä']
    jyv = jyv.to_crs(epsg=3067)
    jyv.drop(['kunta', 'vuosi', 'nimi', 'name', 'namn', 'bbox'], axis=1, inplace=True)
    jyvgeotagged = gpd.overlay(gdf, jyv, how='intersection')

    return jyvgeotagged


if __name__ == '__main__':

    # Download language models
    stanza.download(lang='en')
    stanza.download(lang='fi')
    stanza.download(lang='et')

    # Create pipelines
    nlp_en = create_pipeline('en', 'ewt')
    nlp_fi = create_pipeline('fi', 'tdt')
    nlp_et = create_pipeline('et', 'edt')

    # Create a geodataframe for final output
    final_df = gpd.GeoDataFrame()

    # Read all tweets
    df = pd.read_pickle('all_data.pkl')
    #df = df[0:10000]

    # Separate Finnish, English and Estonian to separate dataframes
    df_fi = df[df['lang']=='fi']
    df_en = df[df['lang']=='en']
    df_et = df[df['lang']=='et']

    # Lemmatize tweet text, using correct language pipeline
    results_fi = create_lemmas_lambda(df_fi, nlp_fi)
    results_en = create_lemmas_lambda(df_en, nlp_en)
    results_et = create_lemmas_lambda(df_et, nlp_et)

    # Retrieve sports related tweets based on keyword lists
    sportslist_fi = ['kävely', 'kävellä', 'käveleminen' 'juoksu', 'juosta', 'juokseminen', 'hiihto', 'hiihtää', 'hiihtäminen',
                 'lenkki', 'lenkkeily', 'lenkkeillä', 'treenata', 'treenaaminen', 'urheilla', 'meloa', 'melonta', 'soutaa',
                 'soutaminen', 'patikointi', 'patikoida', 'patikoiminen',
                  'treeni', 'urheilu', 'liikunta', 'pyörä', 'pyöräily', 'pyöräillä', 'pyöräileminen', 'jääkiekko', 'hockey',
                  'jalkapallo', 'tennis', 'tanssi', 'tanssia', 'tanssiminen', 'hiki', 'hikoilla', 'sulkapallo',
                     'sähly', 'salibandy', 'lentopallo',  'lentis', 'luistella', 'luisteleminen', 'luistelu', 'kuntosali',
                     'koris','futis', 'koripallo', 'uinti', 'uida', 'uiminen', 'kajakki', 'pujehtia', 'purjehdus', 'lätkä', 'jooga',
                     'squash', 'kössi', 'pingis', 'pöytätennis']

    sports_fi = get_sports_tweets(results_fi, sportslist_fi)

    sportslist_en = ['running', 'run', 'walk', 'walking', 'jog' ,'jogging', 'hike', 'hiking', 'trek', 'trekking',
                  'bicycle', 'bike', 'biking','cycling', 'exercise', 'exercising', 'ski', 'skiing', 'skate', 'skating',
                  'workout', 'training', 'sport', 'sporting', 'canoe', 'canoeing', 'ice-hockey', 'basketball',
                 'hockey', 'football', 'tennis', 'dance', 'dancing', 'rowing', 'sweat', 'sweating', 'badminton',
                 'floorball', 'volley', 'volleyball',  'beach volley', 'yoga','swimming', 'swim', 'sail', 'sailing',
                     'kayak', 'kayaking', 'squash', 'tabletennis']


    sports_en = get_sports_tweets(results_en, sportslist_en)


    sportslist_et = ['jooksmine', 'jooksma', 'jooks', 'kõndimine', 'kõnd', 'kõndima', 'jalutama', 'jalutus',
                 'jalutamine', 'sörkimine', 'sörkima', 'sörk', 'sörksjooks', 'matk', 'matkamine', 'matkama',
                   'jalgratas', 'jalgrattasõit', 'rattasõit', 'treening', 'treenima', 'võimlema', 'võimlemine',
                 'uisutamine', 'uisutama', 'suusatama', 'suusatamine', 'sportima', 'sportimine', 'trenn', 'sport',
                 'jõusaal', 'võimla', 'spordihall', 'spordisaal', 'korvpall', 'koss', 'kanuu',  'kanuutama',
                 'kanuutamine', 'kanuusõit', 'jäähoki', 'hoki', 'jalgpall', 'jalka', 'tennis', 'tants',
                 'tantsimine', 'tantsima', 'sõudmine', 'sõudma', 'aerutama', 'aerutamine', 'higi', 'higistama',
                 'higistamine', 'sulgpall', 'bädminton', 'saalihoki', 'volle', 'rannavolle', 'võrkpall',
                 'rannavõrkpall', 'joogatama', 'jooga', 'ujuma', 'ujumine', 'meresüst',  'kajakisõit', 'purjetama',
                 'purjetamine', 'squash', 'seinatennis', 'lauatennis']


    sports_et = get_sports_tweets(results_et, sportslist_et)

    # Combine dataframes of sports tweets
    sports = sports_en.append(sports_fi)
    sports = sports.append(sports_et)

    # Figure out which sports tweets already have geotags
    sportstogeocode = sports[sports['geom'].isna()]
    sportsgeotagged = sports[~(sports['geom'].isna())]

    # Read in gazetteer
    jyvnames = gpd.read_file(r'jyv_gazetteer.gpkg')
    # Geocode the tweets without geolocation
    sportsjyv = geocode(sportstogeocode, jyvnames)

    # Parse points from geotagged tweets
    sportsgeotagged_jyv = parse_points(sportsgeotagged)

    # Combine back to the geocoded tweets
    if len(sportsjyv) > 0:
        sportsjyv_combined = sportsjyv.append(sportsgeotagged_jyv)
        final_df = final_df.append(sportsjyv_combined)
    else:
        final_df = final_df.append(sportsgeotagged_jyv)

    # Drop columns of lists
    final_df = final_df.drop(['lemmas'], axis=1)
    final_df = final_df.drop(['geom'], axis=1)

    if len(final_df) > 0:

        final_df.to_file('finaloutput_lambda.gpkg', driver='GPKG')
        print('---Geopackage created---')
    else:
        print('--- Final dataframe is empty ---')

    print('Time it took in minutes: ')
    print((time.time()-script_start)/60)
