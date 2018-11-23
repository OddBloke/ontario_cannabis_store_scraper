import csv
import datetime
import json
import time

import scraperwiki
import scrapy
from scrapy.crawler import CrawlerProcess
from slimit import ast
from slimit.parser import Parser
from slimit.visitors import nodevisitor
from sqlalchemy import create_engine
from sqlalchemy import (
    Column, Float, ForeignKeyConstraint, Integer, Table, Text, and_, exists)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


Base = declarative_base()


class ProductMixin(object):
    sku = Column(Text)
    url = Column(Text)
    # TODO: Make brand a ForeignKey
    brand = Column(Text, primary_key=True)
    name = Column(Text, primary_key=True)
    price = Column(Float)
    description = Column(Text)
    # TODO: Make type a ForeignKey
    type = Column(Text)
    image = Column(Text)
    plant_type = Column(Text)
    terpenes = Column(Text)

    thc_low = Column(Integer)
    thc_high = Column(Integer)
    cbd_low = Column(Integer)
    cbd_high = Column(Integer)

    standalone_price = Column(Integer)
    standalone_availability = Column(Integer)
    _0_5g_price = Column('0.5g_price', Integer)
    _0_5g_availability = Column('0.5g_availability', Integer)
    _1g_price = Column('1g_price', Integer)
    _1g_availability = Column('1g_availability', Integer)
    _1_25g_price = Column('1.25g_price', Integer)
    _1_25g_availability = Column('1.25g_availability', Integer)
    _1_5g_price = Column('1.5g_price', Integer)
    _1_5g_availability = Column('1.5g_availability', Integer)
    _2_5g_price = Column('2.5g_price', Integer)
    _2_5g_availability = Column('2.5g_availability', Integer)
    _3_5g_price = Column('3.5g_price', Integer)
    _3_5g_availability = Column('3.5g_availability', Integer)
    _5g_price = Column('5g_price', Integer)
    _5g_availability = Column('5g_availability', Integer)
    _7g_price = Column('7g_price', Integer)
    _7g_availability = Column('7g_availability', Integer)
    _15g_price = Column('15g_price', Integer)
    _15g_availability = Column('15g_availability', Integer)


class ProductListing(Base, ProductMixin):
    __tablename__ = 'data'


class HistoricalListing(Base, ProductMixin):
    __tablename__ = 'history'

    timestamp = Column(Integer, primary_key=True)


class HistoricalProductAvailability(Base):
    # This table stores the availability data in way that allows for easier
    # counting of remaining quantities

    __tablename__ = 'history_availability'
    timestamp = Column(Integer, nullable=False,primary_key=True)
    brand = Column(Text, nullable=False, primary_key=True)
    name = Column(Text, nullable=False, primary_key=True)
    size = Column(Float, primary_key=True)
    availability = Column(Integer)
    # TODO: Backfill prices from history table
    price = Column(Integer)

    __table_args__ = (
        ForeignKeyConstraint(
            ['timestamp', 'brand', 'name'],
            ['history.timestamp', 'history.brand', 'history.name']),
    )


def _get_db_session():
    engine = create_engine('sqlite:///data.sqlite')
    Base.metadata.create_all(engine)
    return sessionmaker(engine)()



TIMESTAMP = int(time.time())


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

    @property
    def slimit_parser(self):
        parser = getattr(self, '_slimit_parser', None)
        if parser is None:
            parser = Parser()
            self._slimit_parser = parser
        return parser

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
            '/@data-full-text').extract_first().strip()
        result['type'] = response.xpath(
            './/nav[contains(@class, "breadcrumbs")]/a/text()')[-2].extract()
        result['image'] = response.xpath(
            '//div[@class="product-images__slide"]/img/@src').extract_first()

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

        shopify_script_content = response.xpath(
            '//script[contains(text(), "var meta =")]/text()').extract_first()
        tree = self.slimit_parser.parse(shopify_script_content)
        for node in nodevisitor.visit(tree):
            if node.to_ecma().startswith('"variants":'):
                variants = json.loads(node.right.to_ecma())
                break
        else:
            raise Exception('No variants found')
        variant_dict = {
            d['id']: {'size': d['public_title'], 'price': d['price']}
            for d in variants}

        inventory_script_content = response.xpath(
            '//script[contains(text(), "var inventory_quantities =")]'
            '/text()').extract_first()
        # slimit chokes on the full content, so just use the line we care about
        inventory_quantities_line = (line for line in inventory_script_content.splitlines() if 'var inventory_quantities' in line).next()
        tree = self.slimit_parser.parse(inventory_quantities_line)
        for node in nodevisitor.visit(tree):
            if node.to_ecma().startswith('inventory_quantities ='):
                # This gets us inventory_quantities = {...}; the {...} uses
                # integer keys, so we can't just json.loads it; pull out the
                # values explicitly instead
                for assign in node.initializer.children():
                    id_, quantity = (
                        int(assign.left.to_ecma()), int(assign.right.to_ecma()))
                    if id_ not in variant_dict:
                        raise Exception(
                            '{} not in {}'.format(id_, variant_dict))
                    variant_dict[id_]['availability'] = quantity
        result['variants'] = {
            variant['size']: {'price': variant['price'],
                              'availability': variant['availability']}
            for variant in variant_dict.values()
        }

        if result['url'] == 'https://ocs.ca/products/great-white-shark-2':
            # Fixup a duplicate name in the OCS site
            result['name'] = 'Mazar x G.W.S.'

        # TODO: GTIN
        print(result)

        # Copy over data that is directly supported in SQLite
        sqlite_data = {
            k: v for k, v in result.items()
            if k in ['url', 'brand', 'name', 'sku', 'price', 'description',
                     'type', 'plant_type', 'image']}
        for range_type in ['thc', 'cbd']:
            low, high = result['{}_range'.format(range_type)]
            sqlite_data['{}_low'.format(range_type)] = low
            sqlite_data['{}_high'.format(range_type)] = high
        sqlite_data['terpenes'] = ','.join(result['terpenes'])

        session = _get_db_session()

        for size, variant_dict in result['variants'].items():
            if size is None:
                prefix = 'standalone_'
            else:
                prefix = '_{}_'.format(size.replace('.', '_'))
            if size in ['0.5g', '1g', '1.25g', '1.5g', '2.5g', '3.5g', '5g',
                        '7g', '15g', None]:
                # Only populate the legacy sizes
                sqlite_data[prefix + 'price'] = variant_dict['price']
                sqlite_data[prefix + 'availability'] = (
                    variant_dict['availability'])
            if size is not None:
                session.add(HistoricalProductAvailability(
                    timestamp=TIMESTAMP,
                    brand=result['brand'],
                    name=result['name'],
                    size=float(size.strip('g')),
                    availability=int(variant_dict['availability']),
                    price=int(variant_dict['price']),
                ))

        session.add(ProductListing(**sqlite_data))

        sqlite_data['timestamp'] = TIMESTAMP
        session.add(HistoricalListing(**sqlite_data))
        session.commit()


def do_fixups():
    print datetime.datetime.now().isoformat(), 'Starting fixups...'


if __name__ == '__main__':
    do_fixups()
    # data should only contain the data from the latest run
    scraperwiki.sqlite.execute('DROP TABLE data')
    process = CrawlerProcess()
    process.crawl(OcsSpider)
    process.start()
