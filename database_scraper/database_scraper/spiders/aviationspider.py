import scrapy
from database_scraper.items import AviationDatabaseItem, WikibaseItem
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor

class AviationspiderSpider(CrawlSpider):
    name = "aviationspider"
    allowed_domains = ["aviation-safety.net"]
    start_urls = ["https://aviation-safety.net/database"]
    rules = (
        Rule(
              LinkExtractor(allow=('/wikibase/', )), callback='parse_wikibase_page'),
        Rule(
              LinkExtractor(allow=('/year/', )), callback='next_page')
    )
                              
    def parse(self, response):
    ## This function will iterate over all of the year in the starting page. 
        for year in response.css('a[href^="/database/year"]'):
            relative_url = year.css('a').attrib['href']
            next_year_url = 'https://aviation-safety.net' + relative_url

            if next_year_url is not None:
                yield scrapy.Request(url=next_year_url, callback=self.parse_database_page)
            else:
                yield scrapy.Request(url=next_year_url, callback=self.next_page)
            
    def next_page(self, response):
        ## This function will look for the page numbers
        for page in response.css('a[href^="/database/year"]'):
                relative_url = page.css('a').attrib['href']
                next_page_url = 'https://aviation-safety.net' + relative_url
                
                if next_page_url is not None:
                    yield scrapy.Request(url=next_page_url, callback=self.parse_database_page)
                else:
                    pass

    def parse_database_page(self, response):
    ## This function is to loop over the each of the links that goes into the wikibase page
        table_rows = response.xpath('//*[@class="hp"]//tr')
        for row in table_rows[1:]:
                relative_wikibase_url = row.xpath('td[1]//span//a/@href').extract_first()
                wikibase_url = 'https://aviation-safety.net' + relative_wikibase_url
                if wikibase_url is not None:
                    yield scrapy.Request(url=wikibase_url, callback=self.parse_wikibase_page)
                else:
                    continue
   
        
    def parse_wikibase_page(self, response):
        table_rows = response.xpath('//table//tr') 
        item = WikibaseItem()  
        item['ID'] = response.xpath('//div[@class="pagetitle"]/text()').extract_first()
        item['date'] = table_rows.xpath('//td[text() = "Date:"]/following-sibling::td/text()').extract_first()
        item['time'] = table_rows.xpath('//td[text() = "Time:"]/following-sibling::td/text()').extract_first()
        item['type'] = table_rows.xpath('//td[text() = "Type:"]/following-sibling::td/a/text()').extract_first()
        item['owner_operator'] = table_rows.xpath('//td[text() = "Owner/operator:"]/following-sibling::td/text()').extract_first()
        item['registration'] = table_rows.xpath('//td[text() = "Registration:"]/following-sibling::td/text()').extract_first()
        item['MSN'] = table_rows.xpath('//td[text() = "MSN:"]/following-sibling::td/text()').extract_first()
        item['manufacture_year'] = table_rows.xpath('//td[text() = "Year of manufacture:"]/following-sibling::td/text()').extract_first()
        item['engine_model'] = table_rows.xpath('//td[text() = "Engine model:"]/following-sibling::td/text()').extract_first()
        item['total_airframe_hours'] = table_rows.xpath('//td[text() = "Total airframe hrs:"]/following-sibling::td/text()').extract_first()
        item['cycles'] = table_rows.xpath('//td[text() = "Cycles:"]/following-sibling::td/text()').extract_first()
        item['fatalities'] = table_rows.xpath('//td[text() = "Fatalities:"]/following-sibling::td/text()').extract_first()
        item['other_fatalities'] = table_rows.xpath('//td[text() = "Other fatalities:"]/following-sibling::td/text()').extract_first()
        item['aircraft_damage'] = table_rows.xpath('//td[text() = "Aircraft damage:"]/following-sibling::td/text()').extract_first()
        item['category'] = table_rows.xpath('//td[text() = "Category:"]/following-sibling::td/text()').extract_first()
        item['accident_location'] = table_rows.xpath('//td[text() = "Location:"]/following-sibling::td/text()').extract_first()
        item['accident_location_country'] = table_rows.xpath('//td[text() = "Location:"]/following-sibling::td/a/text()').extract_first()
        item['phase'] = table_rows.xpath('//td[text() = "Phase:"]/following-sibling::td/text()').extract_first()
        item['nature'] = table_rows.xpath('//td[text() = "Nature:"]/following-sibling::td/text()').extract_first()
        item['depature_airport'] = table_rows.xpath('//td[text() = "Depature airport:"]/following-sibling::td/text()').extract_first()
        item['destination_airport'] = table_rows.xpath('//td[text() = "Destination airport:"]/following-sibling::td/text()').extract_first()
        item['investigating_agency'] = table_rows.xpath('//td[text() = "Investigating agency:"]/following-sibling::td/text()').extract_first()
        item['confidence_rating'] = table_rows.xpath('//td[text() = "Confidence Rating:"]/following-sibling::td/text()').extract_first()
        yield item




               
            
