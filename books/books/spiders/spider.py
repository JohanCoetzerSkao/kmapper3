import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from books.items import BooksItem

class SpiderSpider(scrapy.Spider):
    name = "spider"
    allowed_domains = ["books.toscrape.com"]
    start_urls = ["http://books.toscrape.com/"]
    #book_crawler = scrapy.crawler.Crawler()

    #def download_file(self, response):
        #page = response.url.split("/")[-2]
        #filename = f'quotes-{page}.html'
        #Path(filename).write_bytes(response.body)
        #self.log(f'Saved file {filename}')

    def parse(self, response):
        all_books = response.xpath('//article[@class="product_pod"]')

        for book in all_books:
            title = book.xpath('.//h3/a/@title').extract_first()
            price = book.xpath('.//div/p[@class="price_color"]/text()').extract_first()
            image_url = self.start_urls[0] + book.xpath('.//img[@class="thumbnail"]/@src').extract_first()
            book_url = self.start_urls[0] + book.xpath('.//h3/a/@href').extract_first()
            yield {
                'title': title,
                'price': price,
                'Image URL': image_url,
                'Book URL': book_url,
            }
            #self.download_file(book)

    def parse_item(self, response):
        file_url = response.css('.downloadline::attr(href)').get()
        file_url = response.urljoin(file_url)
        item = DownfilesItem()
        item['file_urls'] = [file_url]
        yield item
