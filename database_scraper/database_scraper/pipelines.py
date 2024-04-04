# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import re


class DatabaseScraperPipeline:
    def process_item(self, item, spider):

        adapter = ItemAdapter(item)

        #Get the occurence number for the "ID" column
        id_string = adapter.get('ID')
        split_string_array = id_string.split(' ')
        adapter['ID'] = split_string_array[4].split()

        # Accident location to include on the city/town without "-\n\t"
        accident_location_string = adapter.get('accident_location')
        split_string_array = accident_location_string.split('-')
        adapter['accident_location'] = split_string_array[0]

        # Return fatalities and occupants in seperate columns
        accident_location_string = adapter.get('fatalities')
        split_string_array = accident_location_string.split('/')
        fatalities_split = split_string_array[0].split(' ')
        occupant_split = split_string_array[1].split(' ')

        adapter['fatalities_count'] = fatalities_split[1]  
        adapter['occupants_count'] = occupant_split[2] 
                 
        return item
