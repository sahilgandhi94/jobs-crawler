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
    address = scrapy.Field()
    telephone = scrapy.Field()
    email_id = scrapy.Field()
    recruiter_name = scrapy.Field()
    reference_id = scrapy.Field()
    website = scrapy.Field()
    posted_date = scrapy.Field()
    contact_dump = scrapy.Field()

    google_address = scrapy.Field()
    place_id = scrapy.Field()
    international_phone_number = scrapy.Field()
    formatted_phone_number = scrapy.Field()
    google_url = scrapy.Field()
    website = scrapy.Field()
    latitude = scrapy.Field()
    longitude = scrapy.Field()
    pass
