# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class JobItem(scrapy.Item):
    url = scrapy.Field()
    title = scrapy.Field()
    company_name = scrapy.Field()
    experience_requirements = scrapy.Field()
    location = scrapy.Field()
    description = scrapy.Field()
    salary = scrapy.Field()
    industry = scrapy.Field()
    role = scrapy.Field()
    contact = scrapy.Field()
    posted_date = scrapy.Field()
    pass
