#import required packages
import pandas as pd
#import psycopg2
import spacy 
#from spacy_stanza import StanzaLanguage
import spacy_stanza
import os
import stanza
import matplotlib.pyplot as plt
from pyproj import CRS
import geopandas as gpd
from shapely.geometry import Point
#import emojis
import time
#import requests
#import geojson
#import folium
#from folium.plugins import MarkerCluster
import glob

#def get_data(limit = None):
#    """
 #   Retrieves Twitter data from DGL server from table twitter_histories_full_fin_est_fulltext from twitter_histories schema.
  #  
   # Parameters:
    #
    #limit| int, optional: limits the number of records retrieved
    #"""

    # set up database connection- substitute ' ' with own information.
    #con = psycopg2.connect(database='some_new', user=' ', password=' ',
                       #host=' ')
    
    #if no limit is specified, the function retrives all data (38,5 million rows, takes a while)
    #if limit == None:
     #   sql = 'SELECT lang, full_text, geom FROM twitter_histories.twitter_histories_full_fin_est_fulltext;'
        
    #if a limit is specified, retrieves data according to the limit    
    #else:
     #   sql = 'SELECT lang, full_text, geom FROM twitter_histories.twitter_histories_full_fin_est_fulltext LIMIT ' + str(limit) + ';'
        
    # run query to get a pandas df
    #df = pd.read_sql(sql, con)
    
    #print("Retrieved " + str(limit) + " tweets")
    #return df


#function for creating pipelines
def create_pipeline(lang, package_name):
    """
    Creates a stanza pipeline for the language given as argument. 
    
    Parameters:
    
    lang| String: abbreviation for language which pipeline you want to create eg. "fi" or "en". 
    """
    #to avoid an error
    os.environ["KMP_DUPLICATE_LIB_OK"]="TRUE"
    #make the pipeline for tokenizing and lemmatization
    nlp = stanza.Pipeline(lang, processors='tokenize, lemma', package=package_name)
    #nlp = spacy_stanza.StanzaLanguage(nlp_pipeline)
    print("Stanza pipeline created for language: " + lang)
    return nlp

def create_lemmas(df, nlp_lang):
    """
    Lemmatises text in dataframe column 'full_text'. Takes the name of the dataframe and 
    nlp pipeline for the correct language. Supposes that all tweets have the same language.
    Returns the same dataframe with two additional fields: lemmas (a list of lemmas) and 
    lemma_text (text containing only the lemmas)
    
    Parameters:
    
    df| String: Pandas dataframe to lemmatize
    nlp_lang| String: Name of Stanza Pipeline for the language corresponding to the dataframe
    """
    start_time = time.time()
    
    df["lemmas"] = None
    df["lemma_text"] = None

    for i in df.index:
    
        tweet_text = df.loc[i, "full_text"]
        
        #check for emojis and decode them
        #if(emojis.count(tweet_text) > 0):
            #tweet_text = emojis.decode(df.loc[i, "full_text"])
        try:
            #lemmatise the text
            doc = nlp_lang(tweet_text)

            lemmas = []
            lemma_text = ""
	    
            for sentence in doc.sentences:
                for token in sentence.tokens:
                    lemma = token.words[0].lemma
                    lemmas.append(lemma)
                    lemma_text += " " + lemma

            #access the lemmas and add to the dataframe
            """for token in doc.iter_tokens():
                token.lemma_ = token.lemma_.replace("#", "")
                token.lemma_ = token.lemma_.replace("oitta", "oittaa")
                token.lemma_ = token.lemma_.replace("palohe", "paloheinÃ¤")
                lemmas.append(token.lemma_)
                lemma_text += " " + token.lemma_"""
    
            df.at[i, "lemmas"] = lemmas 
            df.at[i, "lemma_text"] = lemma_text
        
            
        except IndexError:
            print("Encountered Index Error")
        
        #print the time it took to lemmatise x amount of tweets
        tweet_count = len(df)
        total_time = round((time.time() - start_time)/60, 3)
        
    #print("--- Lemmatising %s tweets took %s minutes ---" % (tweet_count, total_time))
    return df


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
            #check if keywords are found in lemmas
            count = [lemma for lemma in df["lemmas"][i] if(lemma in keyword_list)]
    
            #append the matched rows to sports dataframe
            if len(count) > 0:
                sports_df = sports_df.append(row)
        
        except TypeError:
            print("Encountered a TypeError")
            
    print(str(len(sports_df)) + " tweets found with sports related keywords")
    return sports_df

#def get_gazetteer():
 #   """
  #  Retrieve GeoNames gazetteer and limit that to contain only the toponyms in Helsinki Metropolitan area.
   # """
    #retrieve the toponyms from Geonames txt file
    #geonames = pd.read_csv(r"FI.txt", sep="\t", header= None)
    #rename the columns
    #geonames.columns = ["geonameid", "name", "asciiname", "alternatenames", "latitude", "longitude", "feature class", 
     #                   "feature code", "country code", "cc2", "admin1 code", "admin2 code", "admin3 code", "admin4 code",             #                    "population", "elevation", "dem", "timezone", "modification date"]
      
    #make shapely points from latitude and longitude and convert to geodataframe
    #geonames["geometry"] = geonames.apply(lambda row: Point(row["longitude"], row["latitude"]), axis=1)
    #geonames = gpd.GeoDataFrame(geonames)

    #reproject geonames to ETRS89 / TM35FIN
    #geonames.crs = CRS.from_epsg(4326).to_wkt()
    #geonames = geonames.to_crs(epsg=3067)

    #get a shapefile of municipalities
    #municip = gpd.read_file(r"data/Finnish_municipalities/SuomenKuntajako_2021_10k.shp",
     #                 crs= CRS.from_epsg(3067).to_wkt())

    #choose the study area municipalities
    #hma = municip.loc[(municip["NAMEFIN"] == "Espoo") | (municip["NAMEFIN"] == "Helsinki") |
      #          (municip["NAMEFIN"] == "Vantaa") | (municip["NAMEFIN"] == "Kauniainen")]

    #hma = gpd.read_file(r"PKS_postinumeroalueet_2020.shp")
    #hma = hma.to_crs(epsg=3067)
    
    #retrieve the placenames in Helsinki Metropolitan area
    #hmanames = gpd.overlay(geonames, hma, how="intersection")
    
    #convert all placenames to small letters for further processing
    #hmanames["name"] = hmanames["name"].str.lower()
    #delete general city names Helsinki and Espoo for ambigous results
    #hmanames = hmanames[(hmanames.name != "helsinki") & (hmanames.name !="espoo")]
    
    #print("Gazetteer ready for use")
    #return hmanames

def geocode(sportstogeocode, hmanames):
    """
    Geocodes the tweets in sportstogeocode dataframe based on the gazetteer saved in hmanames dataframe.
    
    Parameters:
    
    sportstogeocode | String: name of Pandas dataframe with ungeotagged tweets
    hmanames | String: name of GeoPandas dataframe holding gazetteer information
    """
    #create a list of the placenames for comparison purposes
    placenames = [i.lower() for i in list(hmanames["name"])]
    
    #create a new geodataframe which will hold the geocoded tweets
    sportshma = gpd.GeoDataFrame()
    
    for i, row in sportstogeocode.iterrows():
        
        try:
            #search if any lemmas match the list of placenames
            placelist = [lemma.lower() for lemma in sportstogeocode.loc[i, "lemmas"] if(lemma.lower() in placenames)]
    
            #if there are any placenames, retrieve the point for that place and add it to the tweet information
            if len(placelist) > 0:
                x = hmanames.loc[hmanames["name"]== placelist[0], "geometry"].values[0].x
                y = hmanames.loc[hmanames["name"]== placelist[0], "geometry"].values[0].y
                geom = hmanames.loc[hmanames["name"]== placelist[0], "geometry"].values[0]
                row["geometry"] = geom
                row["lon"] = x
                row["lat"] = y
                sportshma = sportshma.append(row)
 
        except TypeError:
            print("Encountered a TypeError")
            
    print(str(len(sportshma)) + " tweets succesfully geocoded")
    return sportshma

def parse_points(sportsgeotagged):
    """
    Takes geotagged tweets and parses the coordinates into points. Returns a geodataframe with the tweets which are geotagged         inside Helsinki Metropolitan area.
    
    Parameters:
    
    sportsgeotagged | String: name of Pandas dataframe with geotagged tweets
    """
    #parse shapely points from coordinates and convert to epsg 3067
    sportsgeotagged["geometry"] = None
    
    
    geom = []
    for x, y in zip(sportsgeotagged['lon'], sportsgeotagged['lat']):
        try:
            geom.append(Point(x, y))
    
        except (ValueError, KeyError):
            geom.append(None)
            print("Value Error or Key Error while parsing points")
        
    sportsgeotagged['geometry'] = geom
    gdf = gpd.GeoDataFrame(sportsgeotagged, geometry="geometry")
    gdf.crs = CRS.from_epsg(4326).to_wkt()
    gdf = gdf.to_crs(epsg=3067)
    
    #intersect with HMA polygon, save the tweets that are inside AOI
    #hma = gpd.read_file(r"PKS_postinumeroalueet_2020.shp")
    #hma = hma.to_crs(epsg=3067)
    
    #hmageotagged = gpd.overlay(gdf, hma, how="intersection")
    #hmageotagged.drop(["Posno", "Toimip", "Toimip_ru", "Nimi", "Nimi_Ru", "Kunta", "Kunta_nro"], axis=1, inplace=True)

    return gdf


def make_interactive_map(sportshma, filename):
    """
    Produces a folium map and saves it to data/outputs/ folder
    
    Parameters: 
    
    sportshma | String : Name of geopandas dataframe containing the geocoded tweets
    filename | String : Filename that you want to give to the output map (.html added automatically)
    """
    # Create a Map instance
    m = folium.Map(location=[60.25, 24.8], tiles = 'cartodbpositron', zoom_start=11, control_scale=True)

    sportshma.crs = "epsg:3067"
    sportshma = sportshma.to_crs("epsg:4326")
    # Get x and y coordinates for each point
    sportshma["x"] = sportshma["geometry"].apply(lambda geom: geom.x)
    sportshma["y"] = sportshma["geometry"].apply(lambda geom: geom.y)

    # Create a list of coordinate pairs
    locations = list(zip(sportshma["y"], sportshma["x"]))
    # Create a folium marker cluster
    marker_cluster = MarkerCluster(locations)

    marker_cluster = MarkerCluster().add_to(m) # create marker clusters

    for i in range(len(sportshma)):
        location = [sportshma['y'][i],sportshma['x'][i]]
        tooltip = sportshma["full_text"][i]
    
    folium.Marker(location, # adding more details to the popup screen using HTML
                  tooltip=tooltip).add_to(marker_cluster)

    # Add marker cluster to map
    marker_cluster.add_to(m)
    
    #save to output folder
    m.save("data/outputs/" + filename + ".html")
    print("Interactive map saved to data/outputs/" + filename + ".html")
    
    return m, sportshma

    


#retrieve tweets
#df = get_data(10000)

#Download stanza nlp models
stanza.download("en")
stanza.download("fi")
stanza.download("sv")

#create pipelines
nlp_en = create_pipeline("en", "ewt")
nlp_fi = create_pipeline("fi", "tdt")
nlp_et = create_pipeline("sv", "talbanken")

#get info from gazetteer
hmanames = gpd.read_file(r"hmagazetteer.shp")

#create a geodataframe for final output
final_df = gpd.GeoDataFrame()

batchno = 1

for name in glob.glob(r"chunk*"):
   
    print("Processing batch " + str(batchno) + "/ 77")
    df = pd.read_csv(open(name), encoding='utf-8', engine='c')

    #separate English, Finnish and Swedish dataframes
    df_fi = df[df["lang"]=="fi"]
    df_en = df[df["lang"]=="en"]
    df_sv = df[df["lang"]=="sv"]


    #create lemmas for English and Finnish tweets
    df_fi = create_lemmas(df_fi, nlp_fi)
    df_en = create_lemmas(df_en, nlp_en)
    df_sv = create_lemmas(df_sv, nlp_sv)


    #retrieve sports related tweets based on keyword lists
    sportslist_fi = ["juosta", "juoksu", "juokseminen", "lenkkeillä", "lenkki", "lenkkeily", "kävellä", "kävely", "käveleminen",      "patikoida", "patikointi", "patikoiminen", "pyöräillä", "pyörä", "pyöräily", "pyöräileminen"]

    sports_fi = get_sports_tweets(df_fi, sportslist_fi)

    sportslist_en = ["running", "run", "walk", "walking", "jog" ,"jogging", "hike", "hiking", "trek", "trekking", 
                  "bicycle", "bike", "biking","cycling"]


    sports_en = get_sports_tweets(df_en, sportslist_en) 
    
    
    sportslist_sv = ["gående"."joggning","vandring","cykling"]

    sports_sv = get_sports_tweets(df_sv, sportslist_sv)

    #combine lists of sports tweets
    sports = sports_en.append(sports_fi)
    sports = sports.append(sports_sv)

    #here figure out which sports tweets already have geotags
    sportstogeocode = sports[sports['geom'].isna()]
    sportsgeotagged = sports[~(sports['geom'].isna())]


    #geocode the tweets without geolocation
    sportshma = geocode(sportstogeocode, hmanames)

    #combine back to the geocoded tweets and save to shapefile
    sportshma = sportshma.append(sportsgeotagged)
    sportshma.to_csv("sports" + str(batchno) + ".csv")
    final_df = final_df.append(sportshma)
    
    batchno += 1
    
final_df = final_df.drop(["lemmas"], axis=1)        
final_df.to_file("finaloutput.shp")

#make a map
#m, sportshma = make_interactive_map(sportshma, sports_map)