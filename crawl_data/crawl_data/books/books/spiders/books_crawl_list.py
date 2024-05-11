from typing import Iterable

import scrapy
from books.items import BooksItem
import re
from scrapy import Request


class BooksCrawlSpider(scrapy.Spider):
    name = "books_crawl"
    allowed_domains = ["books.toscrape.com"]
    start_urls = ["https://books.toscrape.com/catalogue/category/books/default_15/index.html"]
    
    rating_mapping = {
        'One': 1,
        'Two': 2,
        'Three': 3,
        'Four': 4,
        'Five': 5
    }

    def parse(self, response):
        # Lấy thông tin của các quyển sách trên trang hiện tại
        book_crawl = response.xpath("//ol[@class='row']/li/article[@class='product_pod']")
        for book in book_crawl:
            item = BooksItem()
            item["title"] = book.xpath('.//h3/a/@title').get()
            item["img_url"] = response.urljoin(book.xpath('.//img[@class="thumbnail"]/@src').get())
            rating_text = book.xpath('.//p[contains(@class, "star-rating")]/@class').get().split()[-1]
            item["rating"] = self.rating_mapping.get(rating_text, -1)
            item["price"] = float(book.xpath('.//p[@class="price_color"]/text()').get()[1:])
            item["status"] = book.xpath('.//p[contains(@class,"instock")]/text()[normalize-space()]').get().strip()

            # Lấy đường dẫn tới trang chi tiết của từng quyển sách
            detail_url = book.xpath('.//div[@class="image_container"]/a/@href').get()
            if detail_url:
                yield scrapy.Request(url=response.urljoin(detail_url), callback=self.parse_book_detail, meta={"item": item})

        # Lấy đường dẫn tới trang kế tiếp
        next_page = response.xpath('//li[@class="next"]/a/@href').get()
        if next_page:
            yield scrapy.Request(url=response.urljoin(next_page), callback=self.parse)

    def parse_book_detail(self, response):
        item = response.meta["item"]
        item["desc"] = response.xpath('//article[@class="product_page"]/p/text()').get()
        table_rows = response.xpath('//table[@class="table-striped"]/tr')
        for row in table_rows:
            label = row.xpath('.//th/text()').get()
            if label == "UPC":
                item["upc"] = row.xpath('.//td/text()').get()
            elif label == "Product Type":
                item["product_type"] = row.xpath('.//td/text()').get()
            elif label == "Price (excl. tax)":
                item["price_excl"] = float(row.xpath('.//td/text()').get()[1:])
            elif label == "Price (incl. tax)":
                item["price_incl"] = float(row.xpath('.//td/text()').get()[1:])
            elif label == "Tax":
                item["tax"] = float(row.xpath('.//td/text()').get()[1:])
            elif label == "Availability":
                match = re.search(r'\d+', row.xpath('.//td/text()').get())
                item["availability"] = int(match.group())
            elif label == "Number of reviews":
                item["number_of_reviews"] = int(row.xpath('.//td/text()').get())

        yield item
