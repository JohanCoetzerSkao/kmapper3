import scrapy


class SpiderSpider(scrapy.Spider):
    name = "spider"
    allowed_domains = ["www.skao.int"]
    start_urls = ["https://www.skao.int/en/news"]

    def parse(self, response):
        print(response.status)
        # all_news = response.xpath('//div[@class="field__item"]')
        all_news = response.xpath('//div[@class="info"]')
        for news in all_news:
            # title = news.xpath('.//p/text()').extract_first()
            title = news.xpath('.//strong[@class="title"]/a/span/text()').extract_first()
            print("> %s" % title)
            descr = news.xpath('.//div[@class="field__item"]/p/text()').extract_first()
            print(">> %s" % descr)
            link = news.xpath('.//strong[@class="title"]/a/@href').extract_first()
            print(">> %s" % link)
            print()
            yield {
                'title' : title,
                'descr' : descr,
                'url' : link
            }
