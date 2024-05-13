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
import psycopg2


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

        # Removing trailing spaces from aircraft_damage
        aircraft_damage_string = adapter.get('aircraft_damage')
        if aircraft_damage_string != '':
            adapter['aircraft_damage']  = aircraft_damage_string.strip()    
        else:
            adapter['aircraft_damage']  = None

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

        # Convert string  -> datetime
        date_string = adapter.get('date')

        if date_string and not re.match(r'(?:xx|unk\.)', date_string):
            adapter['date'] = datetime.strptime(date_string, '%A %d %B %Y')
        else:
            adapter['date'] = None

        # String -> int "manufacture year"
        manufature_year_string = adapter.get('manufacture_year')
        if manufature_year_string != None:
            adapter['manufacture_year'] = int(manufature_year_string)
        else:
            adapter['manufacture_year'] = None

        
        # "total_airframe_hours" split string -> convert to int
        total_airframe_hours_string = adapter.get('total_airframe_hours')
        if total_airframe_hours_string != None:
            total_airframe_hours_string_split = total_airframe_hours_string.split(' ')
            adapter['total_airframe_hours'] = int(total_airframe_hours_string_split[0])
        elif total_airframe_hours_string == None:
            adapter['total_airframe_hours'] = None

        # "cycles" split string -> convert to int
        cycles_string = adapter.get('cycles')
        if cycles_string != None:
            cycles_string = cycles_string.split(' ')
            adapter['cycles'] = int(cycles_string[0])
        elif cycles_string == None:
            adapter['cycles'] = None
        return item

class DuplicatesPipeline:
    # Drop duplicates that is found
    def __init__(self):
        self.url_seen = set()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if adapter["url"] in self.url_seen:
            raise DropItem(f"Duplicate item found: {item!r}")
        else:
            self.url_seen.add(adapter["url"])
            return item

class SavingToMyPostGresPipeline(object):
    def __init__(self):
        self.connection = psycopg2.connect(
        host = 'localhost',
        user = 'aranfernando',
        database = 'aranfernando',
        port = '5432'
        )
        self.curr = self.connection.cursor()

        self.curr.execute("""
        CREATE TABLE IF NOT EXISTS aviation(
                        id INTEGER,
                        date DATE,
                        occupants_count INTEGER,
                        fatalities_count INTEGER,
                        location VARCHAR(500),
                        url VARCHAR(150),
                        confidence_rating VARCHAR(500),
                        investigating_agency  VARCHAR(500),
                        depature_airport VARCHAR(500),
                        destination_airport VARCHAR(500),
                        nature VARCHAR(50),
                        phase VARCHAR(50),
                        category VARCHAR(50),
                        aircraft_damage VARCHAR(100),
                        other_fatalities INTEGER,
                        cycles INTEGER,
                        total_airframe_hours INTEGER,
                        engine_model VARCHAR(150),
                        manufacture_year INTEGER,
                        MSN VARCHAR(150),
                        registration VARCHAR(150),
                        owner_operator VARCHAR(150),
                        type VARCHAR(150)
        )""")
    
    def process_item(self, item, spider):
        self.store_db(item)
        return item
    
    def store_db(self, item):
        try:
            self.curr.execute(""" insert into aviation(id, date, occupants_count, fatalities_count, location, url, confidence_rating, investigating_agency, depature_airport, destination_airport, 
                            nature, phase, category, aircraft_damage, other_fatalities, cycles, total_airframe_hours, engine_model, manufacture_year, msn, registration, owner_operator, type) values 
                            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                            (
                            item['ID'], 
                            item['date'], 
                            item['occupants_count'],
                            item['fatalities_count'],
                            item['location'], 
                            item['url'], 
                            item['confidence_rating'], 
                            item['investigating_agency'], 
                            item['depature_airport'], 
                            item['destination_airport'],
                            item['nature'], 
                            item['phase'],
                            item['category'], 
                            item['aircraft_damage'], 
                            item['other_fatalities'], 
                            item['cycles'], 
                            item['total_airframe_hours'], 
                            item['engine_model'], 
                            item['manufacture_year'], 
                            item['MSN'], 
                            item['registration'], 
                            item['owner_operator'],
                            item['type']
                            ))
        except BaseException as e:
            print(e)
        self.connection.commit()

    def close_spider(self, spider):
        ## Close cursor & connection to database 
        self.curr.close()
        self.connection.close()


