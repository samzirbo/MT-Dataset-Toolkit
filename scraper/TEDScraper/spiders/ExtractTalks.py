import scrapy


class ExtractTalksSpider(scrapy.Spider):
    """
    Run:
        scrapy crawl ExtractTalks -O talks.jsonl:jsonlines
    
    Description:
        Extracts the languages of each talk from the TED website.
    """
    name = "ExtractTalks"
    allowed_domains = ["www.ted.com"]
    base_url = "https://www.ted.com"
    start_urls = ["https://www.ted.com/talks/quick-list"]

    def parse(self, response):
        pagination = response.css('a.pagination__item.pagination__link::text').getall()
        max_page = max(map(int, pagination))

        for page in range(1, max_page + 1):
            yield scrapy.Request(url=f"{response.url}?page={page}", callback=self.parse_page)

    def parse_page(self, response):
        talks = response.css('div.quick-list__container-row > div.quick-list__row')

        for row in talks:
            url = row.css('div.title a::attr(href)').get()
            name = url.split('/')[-1]
            duration = "".join(row.css('div.col-xs-1::text').getall()).strip()
            if 'h' in duration:
                duration = duration.replace('h', ':').replace('m', '')
                hours, minutes = map(int, duration.split(':'))
                minutes += hours * 60
            else:
                minutes, _ = map(int, duration.split(':'))

            yield scrapy.Request(url=f"{self.base_url}{url}", callback=self.extract_data, meta={'name': name, 'duration': minutes})

    def extract_data(self, response):
        languages = response.css('link[rel="alternate"][hreflang][hreflang!="x-default"]::attr(href)').getall()
        languages = [lang.split('=')[-1] for lang in languages]

        yield {
            'name': response.meta['name'],
            'duration': response.meta['duration'],
            'languages': languages
        }

