import scrapy
import requests
import json
import pprint
import re
from datetime import datetime
from datetime import timedelta

from crawler.items import JobItem

class BabaJobsScrapy(scrapy.Spider):
    name = "babajobs"
    allowed_domains=["babajob.com"]
    start_urls = [ 
        'http://www.babajob.com/Jobs-DataEntry-in-Thane-page-1',
        'http://www.babajob.com/Jobs-OfficeBoy-in-Thane-page-1',
        'http://www.babajob.com/Jobs-Receptionist-in-Thane-page-1',
        'http://www.babajob.com/Jobs-OfficeClerk-in-Thane-page-1',
        'http://www.babajob.com/Jobs-Sales-in-Thane-page-1',
        'http://www.babajob.com/Jobs-DeliveryCollections-in-Thane-page-1',

        'http://www.babajob.com/Jobs-DataEntry-in-Mumbai-page-1',
        'http://www.babajob.com/Jobs-OfficeBoy-in-Mumbai-page-1',
        'http://www.babajob.com/Jobs-Receptionist-in-Mumbai-page-1',
        'http://www.babajob.com/Jobs-OfficeClerk-in-Mumbai-page-1',
        'http://www.babajob.com/Jobs-Sales-in-Mumbai-page-1',
        'http://www.babajob.com/Jobs-DeliveryCollections-in-Mumbai-page-1',
        
    ]

    def parse(self, response):
        if response.xpath('//div[@itemtype="http://schema.org/JobPosting"]').extract_first() is not None:

            count_string = re.findall(r'\d+',response.xpath('//h4[@style="color: #000; margin-top: 15px; margin-left: 17px;"]/span[@style="font-size: 13px"]/text()').extract_first())
            total_count =  int(count_string[0])
            count_per_page = 0

            for each in response.xpath('//div[@itemtype="http://schema.org/JobPosting"]'):
                href = each.xpath('div/div/h2[@class="s-card-title"]/a/@href')
                experience_requirements = each.xpath('div/div/ul/li/text()').extract()[1]
                count_per_page = count_per_page + 1
                url = response.urljoin(href.extract_first())
                req = scrapy.Request(url, callback=self.parse_job_details)

                if 'sponsored' in each.xpath('div/div/h4[@itemtype="http://schema.org/Organization"]//text()').extract_first().strip().lower():
                    req.meta['premium'] = True
                    # posted_date_string = each.xpath('div/div/ul/li/p[@class="info-label-inline info-label-key"]/text()').extract_first()
                    # posted_date_list = posted_date_string.split()
                else:
                    req.meta['premium'] = False
                    posted_date_string = each.xpath('div/div/ul/li/p[@class="info-label-value is-recent"]/text()').extract_first()
                    posted_date_list = posted_date_string.split()
                    if not posted_date_list[1] == 'hours':
                        continue

                req.meta['url'] = url
                req.meta['experience_requirements'] = experience_requirements
                yield req


            nextUrl = ""
            flag = False
            try:
                if int(response.meta['total_items_iterated']) <= int(response.meta['total_count']):
                    flag = True
                    total_items_iterated = int(response.meta['total_items_iterated']) + count_per_page
            except:
                #first page
                flag = True
                total_items_iterated = count_per_page
            finally:
                if not flag:
                    return
                else:
                    url = response.url
                    page_count = re.findall(r'\d+',url)[0]
                    page_count = int(page_count)
                    next_page = str(page_count + 1)
                    nextUrl = re.sub(r'\d+', next_page, url)
                    paginate_req = scrapy.Request(nextUrl, callback=self.parse)
                    paginate_req.meta['total_count'] = total_count
                    paginate_req.meta['total_items_iterated'] = total_items_iterated
                    yield paginate_req



    def parse_job_details(self, response):

        url = response.meta['url'].split('?')[0]

        job = JobItem()

        job['url'] = response.meta['url']
        job['title'] = response.xpath('//div[@class="row"]/div[@class="col-sm-12"]/h1/text()').extract_first()
        job['posted_date'] = self._join(response.xpath('//div[@class="job-title-right"]/div[@class="date-posted"]//text()').extract())
        job['company_name'] = response.xpath('//div[div[@class="col-sm-2 job-label-text"]/img[@alt="Employer picture"]]/div[@class="col-sm-10 job-info-text"]/text()').extract_first()
        job['salary'] = response.xpath('//div[div[@class="col-sm-2 job-label-text"]/img[@alt="Salary"]]/div[@class="col-sm-10 job-info-text"]/text()').extract_first()
        job['location'] = self._strip(response.xpath('//div[div[@class="col-sm-2 job-label-text"]/img[@alt="Location"]]/div[@class="col-sm-10 job-info-text"]/text()').extract_first())
        # google_maps_url = response.xpath('//div[@id="mapRow"]/div[@class="col-sm-10 job-info-text"]/a/@href').extract_first()
        job['description'] = response.xpath('//div[div[@class="col-sm-2 job-label-text"]/img[@alt="Description"]]/div[@class="col-sm-10 job-info-text"]/text()').extract_first()
        job['experience_requirements'] = response.meta['experience_requirements']
        try:
            job['premium'] = response.meta['premium']
        except KeyError:
            job['premium'] = 'NA'

        job['contact_dump'] = 'NA'
        job['recruiter_name'] = 'NA'
        job['reference_id'] = 'NA'
        job['address'] = 'NA'
        job['industry'] = 'NA'
        job['role'] = 'NA'
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

    def _strip(self, s):
        return s.strip().replace("\r\n,","")