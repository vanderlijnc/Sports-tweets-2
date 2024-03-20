

# Sports-tweets-analysis

Analysis code for IJGIS paper entitled: Combining geotagged Twitter data with geoparsing to identify the locations of sports activity. 

## Topic


This repository holds codes for different analysis phases for Twitter sports activities analysis. In the pre-processing folder is the code for limiting the GeoNames gazetteer to the study area. The main analysis folder contains a python script for lemmatizing and using Named Entity Matching to catch the sports tweets. Furthermore, the script goes through the lemmas and tries to look for matches in the gazetteer. If a match is found, the tweet is saved and becomes part of the data. In the post-processing folder, are scripts for merging the final data together and coding the sports mentioned in the tweets into categories.

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

