#!/usr/bin/env Rscript
library(leaflet)
library(htmlwidgets)
library("optparse")

option_list = list(
  make_option(c("-f", "--file"), type="character", default=NULL, 
              help="dataset file name", metavar="character"),
  make_option(c("-o", "--out"), type="character", default="out.txt", 
              help="output file name [default= %default]", metavar="character")
); 

opt_parser = OptionParser(option_list=option_list);
opt = parse_args(opt_parser);

gpscoords = read.csv(opt$file, header = TRUE)

m <- leaflet(data = gpscoords) %>%
  addProviderTiles(providers$OpenStreetMap) %>%
  #addProviderTiles(providers$MapBox) %>% # Using OSM but can use mapbox
  addTiles() %>%  # Add default map tiles
  addMarkers(~lon[1], ~lat[1], popup="Trip Begin") %>%
  addMarkers(~lon[nrow(gpscoords)], ~lat[nrow(gpscoords)], popup="Trip End") %>%
  addPolylines(lng = ~lon, lat = ~lat)
m
saveWidget(m, file=opt$out)

