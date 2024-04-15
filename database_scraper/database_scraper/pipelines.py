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
            adapter['ID'] = None
        else:
            id_array = split_string_array[4]
            adapter['ID'] = int(re.search(r'[0-9]+', id_array.strip()).group(0))
        
        # Accident location to include the city/town without "-\n\t"
        accident_location_string = adapter.get('accident_location')
        split_string_array = accident_location_string.strip().split('-')
        accident_location_string = split_string_array[0]

        # Merge accident_location & accident_location_country
        accident_location_country_string = adapter.get('accident_location_country')
        if accident_location_country_string == None and accident_location_string != None:
            adapter['location'] = accident_location_string.strip()
        elif re.search(r'^[^a-zA-Z]+$', accident_location_string):
            adapter['location'] = ', '.join(["unknown", accident_location_country_string])
        elif accident_location_country_string != None and accident_location_string ==  None:
            adapter['location'] = accident_location_country_string.strip()
        elif accident_location_country_string == None and accident_location_string == None:
            adapter['location'] = "Unknown"
        else:
            adapter['location'] = ', '.join([accident_location_string.strip(), accident_location_country_string])
        
        # Removing trailing spaces from MSN
        MSN_string = adapter.get('MSN')
        if MSN_string != '':
            adapter['MSN']  = MSN_string.strip()   
        else: 
            adapter['MSN']  = None

        # Removing trailing spaces from Registration
        registration_string = adapter.get('registration')
        if registration_string != '':
            adapter['registration']  = registration_string.strip() 
        else:
            adapter['registration']  = None

        # Removing trailing spaces from Phase
        phase_string = adapter.get('phase')
        if phase_string != '':
            adapter['phase']  = phase_string.strip()    
        else:
            adapter['phase']  = None

        # Return fatalities and occupants in seperate columns
        accident_location_string = adapter.get('fatalities')
        split_string_array = accident_location_string.split('/')
        fatalities_split = split_string_array[0].split(' ')
        occupant_split = split_string_array[1].split(' ')
        fatalities = fatalities_split[1]
        occupants = occupant_split[2]

        if fatalities != '' and occupants == '':
            adapter['fatalities_count'] = int(float(fatalities)) 
            adapter['occupants_count'] = None
        elif fatalities == '' and occupants != '':
            adapter['fatalities_count'] = None 
            adapter['occupants_count'] =  int(float(occupants)) 
        elif fatalities == '' and occupants == '':
            adapter['fatalities_count'] = None 
            adapter['occupants_count'] =  None
        
        else:
            adapter['fatalities_count'] = int(float(fatalities)) 
            adapter['occupants_count'] = int(float(occupants))

        # Merge date & time -> Convert string  -> datetime
        date_string = adapter.get('date')
        if date_string != '':
            split_date_string_array = date_string.split(' ')
            match_string = re.match(r'[xx]', split_date_string_array[0])
            if match_string != '':
                date_string = None

        time_string = adapter.get('time')
        if time_string !=  None:
            split_time_string_array = time_string.split(' ')
            match_string = re.match(r'^[a-m]', split_time_string_array[0])
            if match_string is not None:
                time_string = None

        # define a 'LT' time zone:
        tzmapping = {'LT': dateutil.tz.gettz('Europe/Vilnius')}

        if time_string == None and date_string != None:
            date_time_string_parse  = parse(date_string).strftime('%A %d %B %Y')
            adapter['date_time'] = datetime.strptime(date_time_string_parse, '%A %d %B %Y')

        elif date_string == None:
            adapter['date_time'] = None

        elif date_string and time_string != None:
            date_time_string = ' '.join([date_string, time_string])
            date_time_string_parse  = parse(date_time_string, tzinfos=tzmapping).strftime('%A %d %B %Y %H:%M')
            adapter['date_time'] = datetime.strptime(date_time_string_parse.strip(), '%A %d %B %Y %H:%M')

        else:
            date_time_string = ' '.join([date_string, time_string])
            date_time_string_parse  = parse(date_time_string, tzinfos=tzmapping).strftime('%A %d %B %Y %H:%M %Z')
            adapter['date_time'] = datetime.strptime(date_time_string_parse.strip(), '%A %d %B %Y %H:%M %Z')

        # String -> Datetime "manufacture year"
        manufature_year_string = adapter.get('manufacture_year')
        if manufature_year_string != None:
            adapter['date_time'] = datetime.strptime(manufature_year_string, '%Y')
        else:
            adapter['date_time'] = None

        
        # "total_airframe_hours" split string -> convert to int
        total_airframe_hours_string = adapter.get('total_airframe_hours')
        if total_airframe_hours_string != None:
            total_airframe_hours_string_split = total_airframe_hours_string.split(' ')
            adapter['total_airframe_hours'] = int(total_airframe_hours_string_split[0])
        elif total_airframe_hours_string == None:
            adapter['total_airframe_hours'] = None
        return item
    

    
class DuplicatesPipeline:
    # Drop duplicates that is found
    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if adapter["url"] in self.ids_seen:
            raise DropItem(f"Duplicate item found: {item!r}")
        else:
            self.ids_seen.add(adapter["url"])
            return item
