import scrapy
import requests
import json
import pprint
import re
from datetime import datetime
from datetime import timedelta

from crawler.items import JobItem

class IndeedScrapy(scrapy.Spider):
    name = "shine"
    allowed_domains=["shine.com"]
    start_urls = [ 

    'http://www.shine.com/job-search/simple/data-entry/thane/',
    'http://www.shine.com/job-search/simple/office-boy/thane/',
    'http://www.shine.com/job-search/simple/office-administrator/thane/',
    'http://www.shine.com/job-search/simple/computer-operator/thane/',
    'http://www.shine.com/job-search/simple/data-operator/thane/',
    'http://www.shine.com/job-search/simple/field-executive/thane/',
    'http://www.shine.com/job-search/simple/data-collection/thane/',
    'http://www.shine.com/job-search/simple/marketing-executive/thane/',
    'http://www.shine.com/job-search/simple/delivery/thane/',
    'http://www.shine.com/job-search/simple/courier/thane/',
    'http://www.shine.com/job-search/simple/delivery-executive/thane/',
    'http://www.shine.com/job-search/simple/delivery-boy/thane/',

    # 'http://www.shine.com/job-search/simple/data-entry/navi-mumbai/',
    # 'http://www.shine.com/job-search/simple/office-boy/navi-mumbai/',
    # 'http://www.shine.com/job-search/simple/office-administrator/navi-mumbai/',
    # 'http://www.shine.com/job-search/simple/computer-operator/navi-mumbai/',
    # 'http://www.shine.com/job-search/simple/data-operator/navi-mumbai/',
    # 'http://www.shine.com/job-search/simple/field-executive/navi-mumbai/',
    # 'http://www.shine.com/job-search/simple/data-collection/navi-mumbai/',
    # 'http://www.shine.com/job-search/simple/marketing-executive/navi-mumbai/',
    # 'http://www.shine.com/job-search/simple/delivery/navi-mumbai/',
    # 'http://www.shine.com/job-search/simple/courier/navi-mumbai/',
    # 'http://www.shine.com/job-search/simple/delivery-executive/navi-mumbai/',
    # 'http://www.shine.com/job-search/simple/delivery-boy/navi-mumbai/',

    # 'http://www.shine.com/job-search/simple/data-entry/mumbai/',
    # 'http://www.shine.com/job-search/simple/office-boy/mumbai/',
    # 'http://www.shine.com/job-search/simple/office-administrator/mumbai/',
    # 'http://www.shine.com/job-search/simple/computer-operator/mumbai/',
    # 'http://www.shine.com/job-search/simple/data-operator/mumbai/',
    # 'http://www.shine.com/job-search/simple/field-executive/mumbai/',
    # 'http://www.shine.com/job-search/simple/data-collection/mumbai/',
    # 'http://www.shine.com/job-search/simple/marketing-executive/mumbai/',
    # 'http://www.shine.com/job-search/simple/delivery/mumbai/',
    # 'http://www.shine.com/job-search/simple/courier/mumbai/',
    # 'http://www.shine.com/job-search/simple/delivery-executive/mumbai/',
    # 'http://www.shine.com/job-search/simple/delivery-boy/mumbai/',


    ]

    def parse(self, response):
        if response.xpath('//div[@itemtype="http://schema.org/JobPosting"]').extract_first() is not None:
            count_string = re.findall(r'\d+',response.xpath('//div[@class="num_key"]/em/text()').extract_first())
            total_count =  int(count_string[0])
            count_per_page = 1

            for href in response.xpath('//div[@class="search_listingleft"]/a/@href'):
                count_per_page = count_per_page + 1
                url = response.urljoin(href.extract())
                
                posted_date_string = self._join(response.xpath('//div[@class="share_links"]/text()').extract()) 
                posted_date_list = posted_date_string.split();
                posted_date = datetime.strptime(posted_date_list[2], '%d-%b-%Y').date()
                today = datetime.now().date() 
                if (today - posted_date) <= timedelta(1):
                    print (today - posted_date)
                    req = scrapy.Request(url, callback=self.parse_job_details)
                    req.meta['url'] = url
                    yield req

            try:
                if response.meta['total_items_iterated'] <= response.meta['total_count']:
                    next = list(response.url)
                    next[len(response.url)-2] = str(int(next[len(response.url)-2]) + 1)
                    nextUrl = "".join(next)
                    total_items_iterated = int(response.meta['total_items_iterated']) + count_per_page
                else:
                    return
            except:
                #first page
                nextUrl = response.url + str(2) + "/"
                total_items_iterated = count_per_page
            finally:
                paginate_req = scrapy.Request(nextUrl, callback=self.parse)
                paginate_req.meta['total_count'] = total_count
                paginate_req.meta['total_items_iterated'] = total_items_iterated
                yield paginate_req
            


    def parse_job_details(self, response):

        url = response.meta['url'].split('?')[0]


        if response.xpath('//div[@itemtype="http://schema.org/JobPosting"]').extract_first() is None:
            # todo: add logic to handle error pages

            # could not parse this page. add to error item
            # error = ErrorItem()
            # error['url'] = response.meta['url']
            # yield error
            return


        job = JobItem()
        job_posting = response.xpath('//div[@itemtype="http://schema.org/JobPosting"]')[0]
        job['url'] = response.meta['url']
        job['title'] = job_posting.xpath('//h1[@itemprop="title"]/text()').extract_first()
        job['company_name'] = job_posting.xpath('//span[@itemprop="name"]/h2/text()').extract_first()
        job['posted_date'] = job_posting.xpath('//span[@itemprop="datePosted"]/text()').extract_first()
        experience_requirements = job_posting.xpath('//span[@itemprop="experienceRequirements"]/text()').extract()
        job['experience_requirements'] = experience_requirements[0]
        try:
            job['salary'] = experience_requirements[1]
        except Exception:
            job['salary'] = 'NA' 


        job['location'] = self._join(job_posting.xpath('//a[@class="normaljdsnippet jd_location curText cls_jd_primary_location"]/text()').extract(), delimeter=',') #check this out later
        job['role'] = self._join(job_posting.xpath('//span[@itemprop="skills"]/a/text()').extract(), delimeter=',') #
        job['description'] = job_posting.xpath('//span[@itemprop="description"]//text()').extract()
        for i in job['description']:
            if re.match('(http\:\/\/|https\:\/\/)?([a-z0-9][a-z0-9\-]*\.)+[a-z0-9][a-z0-9\-]*$', i):
                job['website'] = i

        job['industry'] = self._join(job_posting.xpath('//span[@itemprop="industry"]/a/text()').extract(), delimeter=',')

        recruiter_details = job_posting.xpath('//div[@class="ropen cls_rect_detail_div"]/ul//text()').extract()
        for i in recruiter_details:
            if re.match('Email\s*(.*)', i):
                job['email_id'] = recruiter_details[recruiter_details.index(i)+1]
            else:
                job['email_id'] = 'NA'
            
            if re.match('Telephone\s*(.*)', i):
                job['telephone'] = recruiter_details[recruiter_details.index(i)+1]
            else:
                job['telephone'] = 'NA'

        job['contact_dump'] = 'NA'
        job['recruiter_name'] = 'NA'
        job['reference_id'] = 'NA'
        job['address'] = 'NA'

        yield job

    
    def _rstrip(self, l):
        return [x.strip().replace("\r\n,","") for x in l]

    def _join(self, l, delimeter=' '):
        return delimeter.join(self._rstrip(l))  # to remove \r\n characters

    def _fetch(self, data, key, subkey=None):
        if key in data.keys():
            if subkey is not None and subkey in data[key].keys():
                return data[key][subkey]
            else:
                return data[key]
        else:
            return 'NA'