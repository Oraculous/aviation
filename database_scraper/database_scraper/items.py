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
    ID = scrapy.Field()
    table_row_count = scrapy.Field()
    date = scrapy.Field()
    type =  scrapy.Field()
    time = scrapy.Field()
    owner_operator = scrapy.Field()
    registration = scrapy.Field()
    MSN = scrapy.Field()
    manufacture_year = scrapy.Field()
    total_airframe_hours = scrapy.Field()
    cycles = scrapy.Field()
    engine_model = scrapy.Field()
    fatalities = scrapy.Field()
    fatalities_count = scrapy.Field()
    occupants_count = scrapy.Field()
    other_fatalities = scrapy.Field()
    aircraft_damage = scrapy.Field()
    category = scrapy.Field()
    accident_location = scrapy.Field()
    accident_location_country = scrapy.Field()
    phase = scrapy.Field()
    nature = scrapy.Field()
    depature_airport = scrapy.Field()
    destination_airport = scrapy.Field()
    investigating_agency = scrapy.Field()
    confidence_rating = scrapy.Field()