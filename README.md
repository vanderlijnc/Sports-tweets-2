<<<<<<< HEAD
 # Sports-tweets-analysis
=======

# Sports-tweets-analysis

Analysis code for JOSIS paper entitled: Combining geotagged Twitter data with geoparsing to identify the locations of sports activity. 

## Topic

<<<<<<< HEAD
This repository holds codes for different analysis phases for Twitter sports activities analysis. In the pre-processing folder is the code for limiting the GeoNames gazetteer to the study area. The main analysis folder contains a python script for lemmatizing and using Named Entity Matching to catch the sports tweets. Furthermore, the script goes through the lemmas and tries to look for matches in the gazetteer. If a match is found, the tweet is saved and becomes part of the data. In the post-processing folder, are scripts for merging the final data together and coding the sports mentioned in the tweets into categories.
=======
This repository holds codes for different analysis phases for Twitter sports activities analysis. In the pre-processing folder, are the codes for retrieving tweets from database, getting LIPAS data from API and limiting the GeoNames gazetteer to the study area. In the same folder, I have also combined the socio-economic data to one compact file and merged that with the location information. The main analysis folder contains a python script for lemmatizing and using Named Entity Matching to catch the sports tweets. Furthermore, the script goes through the lemmas and tries to look for matches in the gazetteer. If a match is found, the tweet is saved and becomes part of the data. In the post-processing folder, are scripts for merging the final data together and coding the sports mentioned in the tweets into categories.
>>>>>>> e4826e6c04b77e89e196dde24d17686b201a1fe7

## Structure of the repository

- Pre-processing
- Main analysis
- Post-processing

## Packages needed

### Python

- geopandas
- pandas
- pyproj
- os
- requests
- geojson
- shapely
- matplotlib
- mapclassify
- statsmodels
- glob
- folium
- numpy
- scipy
- spacy 
- spacy_stanza
- stanza
- psycopg2
- time
<<<<<<< HEAD

>>>>>>> 129b11b9acad7982f91173e3fcfeff1687822f65
=======
>>>>>>> ff02b0bc2e466467dea1d81a8d0ae5be8327c9f4
