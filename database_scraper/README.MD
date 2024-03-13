# Aviation Crawler
This Web Crawler is purely for educational purposes, the data is the sole propriety of the Flight Safety Foundation. It is designed to scrape the aviation safety database utlizing the scrapy package. 

'item_scraped_count': 8882

The ASN Safety Database is updated daily and contains descriptions of airlines, military transport category aircrafts and corporate jet aircrafts safety occurrences since 1919. Airliners are considered here aircraft that are capable of carrying at least 12 passengers.

After the intial run of the code I noticed that not all the table rows are the same. Drawing attention to the screenshot below there appered to be a vast number of different table row counts across some of the wikibase pages. This caused one major problem in the code - some of the assigned item did not get parsed into the respective column names. 

![screenshot1](/Users/aranfernando/analysis/aviation/aviation_analysis/screenshots/screenshot1.png)

The way around this was to use a elif statement in order to pick and chose which indices needed to picked out and differnt table row counts.

