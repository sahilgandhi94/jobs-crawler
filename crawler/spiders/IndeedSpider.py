import scrapy
import requests
import json
import pprint
import re
from datetime import datetime
from datetime import timedelta

from crawler.items import JobItem

class IndeedScrapy(scrapy.Spider):
    name = "indeed"
    allowed_domains=["indeed.co.in"]
    start_urls = [
        'http://www.indeed.co.in/jobs?q=office+boy&l=Thane&start=0',
        'http://www.indeed.co.in/jobs?q=office+administrative&l=Thane&start=0',
        'http://www.indeed.co.in/jobs?q=back+office&l=Thane&start=0',
        'http://www.indeed.co.in/jobs?q=data+entry&l=Thane&start=0',
        'http://www.indeed.co.in/jobs?q=computer+operator&l=Thane&start=0',
        'http://www.indeed.co.in/jobs?q=data+operator&l=Thane&start=0',
        'http://www.indeed.co.in/jobs?q=field+work&l=Thane&start=0',


        'http://www.indeed.co.in/jobs?q=office+boy&l=Mumbai&start=0',
        'http://www.indeed.co.in/jobs?q=office+administrative&l=Mumbai&start=0',
        'http://www.indeed.co.in/jobs?q=back+office&l=Mumbai&start=0',
        'http://www.indeed.co.in/jobs?q=data+entry&l=Mumbai&start=0',
        'http://www.indeed.co.in/jobs?q=computer+operator&l=Mumbai&start=0',
        'http://www.indeed.co.in/jobs?q=data+operator&l=Mumbai&start=0',
        'http://www.indeed.co.in/jobs?q=field+work&l=Mumbai&start=0',


        'http://www.indeed.co.in/jobs?q=office+boy&l=Navi+Mumbai&start=0',
        'http://www.indeed.co.in/jobs?q=office+administrative&l=Navi+Mumbai&start=0',
        'http://www.indeed.co.in/jobs?q=back+office&l=Navi+Mumbai&start=0',
        'http://www.indeed.co.in/jobs?q=data+entry&l=Navi+Mumbai&start=0',
        'http://www.indeed.co.in/jobs?q=computer+operator&l=Navi+Mumbai&start=0',
        'http://www.indeed.co.in/jobs?q=data+operator&l=Navi+Mumbai&start=0',
        'http://www.indeed.co.in/jobs?q=field+work&l=Navi+Mumbai&start=0',
    ]


    def parse(self, response):
        if response.xpath('//div[@itemtype="http://schema.org/JobPosting"]').extract_first() is not None:
            
            for each in response.xpath('//div[@itemtype="http://schema.org/JobPosting"]'):
                if each.xpath('table/tr/td/div[@class="result-link-bar-container"]/div[@class="result-link-bar"]/span[@class="date"]/text()').extract_first() == '1 day ago':
                    if each.xpath('table/tr/td/div[@class="result-link-bar-container"]/div[@class="result-link-bar"]/span[@class="result-link-source"]/text()').extract_first() is None:
                        href = "http://www.indeed.co.in" + each.xpath('h2/a/@href').extract_first()
                        url = response.urljoin(href)
                        req = scrapy.Request(url, callback=self.parse_job_details)
                        req.meta['url'] = url
                        yield req

          
            for x in response.xpath('//div[@class="pagination"]/a'):
                try:
                    if "Next" in x.xpath('span/span/text()').extract_first():
                        nextUrl = "http://www.indeed.co.in" + x.xpath('@href').extract_first()        
                        paginate_req = scrapy.Request(nextUrl, callback=self.parse)
                        yield paginate_req
                except:
                    continue



    def parse_job_details(self, response):

        url = response.meta['url'].split('?')[0]
        
        job = JobItem()
        job_content = response.xpath('//table[@id="job-content"]')[0]
        job['url'] = response.meta['url']
        job['title'] = response.xpath('//div[@id="job_header"]/b/font/text()').extract_first()
        job['company_name'] = response.xpath('//div[@id="job_header"]/span[@class="company"]/text()').extract_first()
        job['location'] = response.xpath('//div[@id="job_header"]/span[@class="location"]/text()').extract_first()
        job['salary'] = response.xpath('//div[@id="job_header"]/span[@style="white-space: nowrap"]/text()').extract_first()
        job['description'] = response.xpath('//span[@id="job_summary"]//text()').extract()
        for i in job['description']:
            if re.match('(http\:\/\/|https\:\/\/)?([a-z0-9][a-z0-9\-]*\.)+[a-z0-9][a-z0-9\-]*$', i):
                job['website'] = i
        
        job['posted_date'] = response.xpath('//div[@class="result-link-bar"]/span[@class="date"]/text()').extract_first()
        
        job['experience_requirements'] = 'NA'
        job['industry'] = 'NA'
        job['role'] = 'NA'
        job['address'] = 'NA'
        job['telephone'] = 'NA'
        job['email_id'] = 'NA'
        job['recruiter_name'] = 'NA'
        job['reference_id'] = 'NA'
        job['contact_dump'] = 'NA'

        yield job
