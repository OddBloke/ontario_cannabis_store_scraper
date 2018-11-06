import scraperwiki
import scrapy
from scrapy.crawler import CrawlerProcess


class OcsSpider(scrapy.Spider):
    name = 'ocs'
    allowed_domains = ['ocs.ca']
    start_urls = ['https://ocs.ca/collections/all-cannabis-products']

    def parse(self, response):
        next_href = response.xpath(
            './/li[@class="pagination_next"]/a/@href').extract_first()
        for product_link in response.xpath(
                './/article/div[1]/a[1]/@href').extract():
            yield response.follow(
                product_link, callback=self.parse_product_page)
        if next_href is not None:
            yield response.follow(next_href)

    def parse_product_page(self, response):
        result = {'url': response.url}

        # Header
        get_header = lambda cls: response.xpath(
            './/header[contains(@class, "product__header")]'
            '/*[contains(@class, "{}")]'
            '/text()'.format(cls)).extract_first().strip()
        result['brand'] = get_header('product__brand')
        result['name'] = get_header('product__title')
        result['sku'] = get_header('product__sku')
        result['price'] = float(get_header('product__price').strip('$'))

        result['description'] = response.xpath(
            './/*[@class="product__info"]/div/p'
            '/@data-full-text').extract_first()
        result['type'] = response.xpath(
            './/nav[contains(@class, "breadcrumbs")]/a/text()')[-2].extract()

        # Properties
        get_property = lambda prop: response.xpath(
            '//ul[@class="product__properties"]//h3[@id="{}-tooltip-1"]'
            '/../p/text()'.format(prop)).extract_first()
        get_range = lambda s: [
            float(p) for p in s.strip().strip('%').split(' - ')]
        thc = get_property('thc')
        result['thc_range'] = get_range(thc) if thc is not None else [0, 0]
        cbd = get_property('cbd')
        result['cbd_range'] = get_range(cbd) if cbd is not None else [0, 0]
        result['plant_type'] = get_property('plant_type')

        result['terpenes'] = response.xpath(
            './/p[@class="terpene__list"]/span/text()').extract()

        # TODO: Availability

        # TODO: GTIN
        print(result)

        # Copy over data that is directly supported in SQLite
        sqlite_data = {
            k: v for k, v in result.items()
            if k in ['url', 'brand', 'name', 'sku', 'price', 'description',
                     'type', 'plant_type']}
        for range_type in ['thc', 'cbd']:
            low, high = result['{}_range'.format(range_type)]
            sqlite_data['{}_low'.format(range_type)] = low
            sqlite_data['{}_high'.format(range_type)] = high
        sqlite_data['terpenes'] = ','.join(result['terpenes'])
        scraperwiki.sqlite.save(unique_keys=['sku'], data=sqlite_data)



if __name__ == '__main__':
    process = CrawlerProcess()
    process.crawl(OcsSpider)
    process.start()
