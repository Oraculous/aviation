# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class DatabaseScraperItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

class AviationDatabaseItem(scrapy.Item):
    accident_date = scrapy.Field()
    aircraft_type = scrapy.Field()
    registration_no = scrapy.Field()
    operator = scrapy.Field()
    fatalities = scrapy.Field()
    location = scrapy.Field()
    damage = scrapy.Field()

class WikibaseItem(scrapy.Item):
    ASN_id =  scrapy.Field()
    time = scrapy.Field()
    owner_operator = scrapy.Field()
    registration = scrapy.Field()
    MSN = scrapy.Field()
    manufacture_year = scrapy.Field()
    engine_model = scrapy.Field()
    fatalities = scrapy.Field()
    aircraft_damage = scrapy.Field()
    category = scrapy.Field()
    location = scrapy.Field()
    phase = scrapy.Field()
    nature = scrapy.Field()
    depature_airport = scrapy.Field()
    depature_airport = scrapy.Field()
    confidence_rating = scrapy.Field()