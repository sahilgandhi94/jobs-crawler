import scrapy
import requests
import json
import pprint

from crawler.items import JobItem

class IndeedScrapy(scrapy.Spider):
    name = "naukri"
    allowed_domains=["naukri.com"]
    start_urls = [
        "http://www.naukri.com/computer-operator-jobs-in-thane"
    ]

    def parse(self, response):
        if response.xpath('//div[@itemtype="http://schema.org/JobPosting"]/a/@href').extract_first() is not None:
            for href in response.xpath('//div[@itemtype="http://schema.org/JobPosting"]/a/@href'):
                url = response.urljoin(href.extract())
                req = scrapy.Request(url, callback=self.parse_job_details)
                req.meta['url'] = url
                yield req

            if response.xpath('//div[@class="pagination"]/a/@href').extract_first() is not None:
                next = response.xpath('//div[@class="pagination"]/a/@href').extract()
                if len(next) > 1 :
                    next = response.urljoin(next[1])
                else:
                    next = response.urljoin(next[0])
                paginate_req = scrapy.Request(next, callback=self.parse)
                next_paginate_number = int(str(next[str(next).rfind('-')+1:]))
                try:
                    current_paginate_number = int(response.meta['current_paginate_number'])                    
                except KeyError:
                    # current page is first page of pagination
                    current_paginate_number = 1
                finally:
                    if current_paginate_number > next_paginate_number:
                        return
                    paginate_req.meta['current_paginate_number'] = next_paginate_number
                    yield paginate_req


    def parse_job_details(self, response):
        # extract job-id
        url = response.meta['url'].split('?')[0]
        index = url.rfind('-')
        jid = url[index+1:]

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
        job['title'] = self._join(job_posting.xpath('//h1[@itemprop="title"]/text()').extract())
        job['company_name'] = self._join(job_posting.xpath('//a[@itemprop="hiringOrganization"]/text()').extract())
        job['experience_requirements'] = self._join(job_posting.xpath('//span[@itemprop="experienceRequirements"]/text()').extract())

        location = job_posting.xpath('//em[@itemprop="jobLocation"]')[0]
        job['location'] = self._join(location.xpath('//div[@itemprop="name"]/a/text()').extract(), delimeter=', ')

        job['description'] = self._join(job_posting.xpath('//ul[@itemprop="description"]//text()').extract())
        job['salary'] = self._join(job_posting.xpath('//span[@itemprop="baseSalary"]/text()').extract())
        job['industry'] = self._join(job_posting.xpath('//span[@itemprop="industry"]/a/text()').extract(), delimeter=', ')

        job['role'] = ''
        for role in job_posting.xpath('//p/span[@itemprop="occupationalCategory"]'):
            if role.xpath('a/text()').extract_first() is not None:
                job['role'] += self._join(role.xpath('a/text()').extract(), delimeter=', ')
            else:
                job['role'] += ', ' + self._join(role.xpath('text()').extract(), delimeter=', ')
        
        # parsing for contact details
        contact_url = 'http://www.naukri.com/jd/contactDetails?file=' + str(jid)
        r = requests.get(contact_url)
        if r.status_code == 200:
            job['contact'] = json.dumps(r.json()['fields'])
        else:
            job['contact'] = 'NA'

        job['posted_date'] = job_posting.xpath('//div[@class="sumFoot"]//text()').re('Posted\s*(.*)')

        yield job

    def _rstrip(self, l):
        return [x.strip().replace("\r\n,","") for x in l]

    def _join(self, l, delimeter=' '):
        return delimeter.join(self._rstrip(l))  # to remove \r\n characters


