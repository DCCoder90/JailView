# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class ChargeItem(scrapy.Item):
    statute = scrapy.Field()
    casenumber = scrapy.Field()
    charge = scrapy.Field()
    degree = scrapy.Field()
    level = scrapy.Field()
    bond = scrapy.Field()
        
class RecordItem(scrapy.Item):
    bookingnum = scrapy.Field()
    mninum = scrapy.Field()
    ageonbooking = scrapy.Field()
    bookdate = scrapy.Field()
    bondamount = scrapy.Field()
    address = scrapy.Field()
    imageurl = scrapy.Field()
    charges = scrapy.Field()
    

class JailviewItem(scrapy.Item):
    identifier = scrapy.Field()
    record = scrapy.Field()
    pass