# Aviation Crawler
> [!WARNING]
> This Web Crawler is purely for educational purposes - all data is property of [Flight Safety Foundation](https://flightsafety.org). 

## Introduction

### ASN Database

The [Aviation Safety Network](https://aviation-safety.net) Database is updated daily and contains descriptions of airlines, military transport category aircrafts and corporate jet aircrafts safety occurrences since 1919. Airliners considered are capable of carrying at least 12 passengers.

### Scrapy Crawler

This web crawler is designed to scrape websites into a structured json file for further exploratory data analysis for educational purposes. 

The crawler used is called [Scrapy](https://github.com/scrapy/scrapy) and it is a web-scraping framework designed to crawl websites and extract structured data from their pages. More information can be found on the [Scrapy Homepage](https://scrapy.org/)

## Item Pipelines
Item pipelines were used to clean the extracted data before being dumped onto a json file.

The "ID" column were cleaned to only include the wikibase number and "accident_location" was cleaned to ensure that "-\n\t" was not included in the final output. 

In addition the Fatalities columns was split into two other columns each designating the number of fatalities and the number of occupants. This is to distinguish the difference in casualties. 

The phase, registration and MSN columns was observed to contain trailing spaces - these were cleaned using a simple strip function.

Next was to convert the date column from string to DateTime.

The cleaned dataset was then uploaded into PostGreSQL. A copy of the dataset was also saved in the repository as aviaition_database_output_cleaned. 

The next step is to perform exploratory dataanalysis (EDA) on the data to gather insights and perform more data cleaning if required. 




















