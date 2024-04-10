# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import re
from datetime import datetime
from dateutil.parser import parse
import dateutil
from scrapy.exceptions import DropItem


class DatabaseScraperPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        #Get the occurence number for the "ID" column
        id_string = adapter.get('ID')
        split_string_array = id_string.split(' ')
        if len(split_string_array) == 1:
            adapter['ID_test'] = None
        else:
            id_array = split_string_array[4]
            adapter['ID'] = int(re.search(r'[0-9]+', id_array.strip()).group(0))
        return item
    
class DuplicatesPipeline:
    # Drop duplicates that is found
    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if adapter["ID"] in self.ids_seen:
            raise DropItem(f"Duplicate item found: {item!r}")
        else:
            self.ids_seen.add(adapter["ID"])
            return item
