import scrapy
from database_scraper.items import AviationDatabaseItem, WikibaseItem

class AviationspiderSpider(scrapy.Spider):
    name = "aviationspider"
    allowed_domains = ["aviation-safety.net"]
    start_urls = ["https://aviation-safety.net/database"]
    
                              
    def parse(self, response):

        for year in response.css('a[href^="/database/year"]'):
            relative_url = year.css('a').attrib['href']
            next_year_url = 'https://aviation-safety.net' + relative_url

            if next_year_url is not None:
                yield scrapy.Request(url=next_year_url, callback=self.parse_database_page)
    
    def parse_database_page(self, response):
        table_rows = response.xpath('//*[@class="hp"]//tr')
        for row in table_rows[1:]:
                item = AviationDatabaseItem()
                item['accident_date'] = row.xpath('td[1]//text()').extract_first(),
                item['aircraft_type'] = row.xpath('td[2]//text()').extract_first(),
                item['registration_no'] = row.xpath('td[3]//text()').extract_first(),
                item['operator'] = row.xpath('td[4]//text()').extract_first(),
                item['fatalities'] = row.xpath('td[5]//text()').extract_first(),
                item['location'] = row.xpath('td[6]//text()').extract_first(),
                item['damage'] = row.xpath('td[8]//text()').extract_first(),
                relative_wikibase_url = table_rows.xpath('td[1]//span//a/@href').extract_first()
                wikibase_url = 'https://aviation-safety.net' + relative_wikibase_url
                if wikibase_url is not None:
                    yield scrapy.Request(url=wikibase_url, callback=self.parse_wikibase_page)
                

    def parse_wikibase_page(self, response):
        table_rows = response.xpath('//table//tr')
        for table_row in table_rows[1:]
        item = WikibaseItem()
        item['date'] = table_row[0].xpath('td[2]/text()').extract_first()
        item['time'] = table_row[1].xpath('td[@class="desc"]/text()').extract_first()
        item['type'] = table_row[2].css('td a ::text').extract_first()
        item['owner_operator'] = table_row[3].xpath('td[@class="desc"]/text()').extract_first()
        item['registration'] = table_row[4].xpath('td[@class="desc"]/text()').extract_first()
        item['MSN'] = table_row[5].xpath('td[@class="desc"]/text()').extract_first()
        item['manufacture_year'] =  table_row[6].xpath('td[@class="desc"]/text()').extract_first()
        item['engine_model'] = table_row[7].xpath('td[@class="desc"]/text()').extract_first()
        item['fatalities'] = table_row[8].xpath('td[@class="desc"]/text()').extract_first()
        item['aircraft_damage'] = table_row[9].xpath('td[@class="desc"]/text()').extract_first()
        item['category'] = table_row[10].xpath('td[@class="desc"]/text()').extract_first()
        item['location'] = table_row[11].xpath('td[@class="desc"]/text()').extract_first()
        item['phase'] = table_row[12].xpath('td[@class="desc"]/text()').extract_first()
        item['nature'] = table_row[13].xpath('td[@class="desc"]/text()').extract_first()
        item['depature_airport'] = table_row[14].xpath('td[@class="desc"]/text()').extract_first()
        item['depature_airport'] = table_row[15].xpath('td[@class="desc"]/text()').extract_first()
        item['confidence_rating'] = table_row[16].xpath('td[@class="desc"]/text()').extract_first()
        yield item
        
                
            
