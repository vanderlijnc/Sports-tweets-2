![thesis_workflow-Page-2](https://user-images.githubusercontent.com/37457729/124478419-d5bdd280-ddad-11eb-83a3-628608885307.png)
# Sports-tweets-analysis

Analysis code for MSc thesis "Twitter as an indicator of sports activities in Helsinki Metropolitan Area".

## Topic

This repository holds codes for different analysis phases for Twitter sports activities analysis. In the pre-processing folder, are the codes for retrieving tweets from database, getting LIPAS data from API and limiting the GeoNames gazetteer to the study area. In the same folder, I have also combined the socio-economic data to one compact file and merged that with the location information. The main analysis folder contains a python script for lemmatizing and using Named Entity Matching to catch the sports tweets. Furthermore, the script goes through the lemmas and tries to look for matches in the gazetteer. If a match is found, the tweet is saved and becomes part of the data. In the post-processing folder, are scripts for merging the final data together and coding the sports mentioned in the tweets into categories, post-processing for finding correlations from Facebook survey answer data and an R script for variable preselection and Ordinary Least Squares regression.

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

### R

- foreign
- spatialreg
- spdep
- car
- AICcmodavg
- corrplot
