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
                # relative_wikibase_url = table_rows.xpath('td[1]//span//a/@href').extract_first()
                # wikibase_url = 'https://aviation-safety.net' + relative_wikibase_url
                # if wikibase_url is not None:
                #     yield scrapy.Request(url=wikibase_url, callback=self.parse_wikibase_page)
                yield item
                

    # def parse_wikibase_page(self, response):
    #     description = response.xpath('//*[@class="desc"]')
    #     for description in description:
    #         item = WikibaseItem()
    #         item['time'] = description[0].css('::text').extract_first()
    #         item['type'] = description[1].css('a::text').extract_first()
    #         item['owner_operator'] = description[2].css('::text').extract_first()
    #         item['registration'] = description[3].css('::text').extract_first()
    #         item['MSN'] = description[4].css('::text').extract_first()
    #         item['manufacture_year'] = description[5].css('::text').extract_first()
    #         item['engine_model'] = description[6].css('::text').extract_first()
    #         item['fatalities'] = description[7].css('::text').extract_first()
    #         item['aircraft_damage'] = description[8].css('::text').extract_first()
    #         item['category'] = description[9].css('::text').extract_first()
    #         item['location'] = description[10].css('::text').extract_first()
    #         item['phase'] = description[11].css('::text').extract_first()
    #         item['nature'] = description[12].css('::text').extract_first()
    #         item['depature_airport'] = description[13].css('::text').extract_first()
    #         item['depature_airport'] = description[14].css('::text').extract_first()
    #         item['confidence_rating'] = description[15].css('::text').extract_first()
    #         yield item
            
                    
                
