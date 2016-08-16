import scrapy

from crawler.items import JobItem

class OLX(scrapy.Spider):
    name = "olx"
    allowed_domains = ["olx.in"]
    start_urls = [
        # 'https://www.olx.in/mumbai/customer-service/?page=1',
        # # 'https://www.olx.in/mumbai/online-part-time/?page=1',
        # 'https://www.olx.in/mumbai/marketing/?page=1',
        # 'https://www.olx.in/mumbai/advertising-pr/?page=1',
        # 'https://www.olx.in/mumbai/hotels-tourism/?page=1',
        # 'https://www.olx.in/mumbai/human-resources/?page=1',
        # 'https://www.olx.in/mumbai/clerical-administration/?page=1',
        # 'https://www.olx.in/mumbai/sales/?page=1',
        # 'https://www.olx.in/mumbai/manufacturing/?page=1',
        # 'https://www.olx.in/mumbai/part-time/?page=1',
        # 'https://www.olx.in/mumbai/other-jobs/?page=1',
        # 'https://www.olx.in/mumbai/it/?page=1',
        # 'https://www.olx.in/mumbai/education/?page=1',
        # 'https://www.olx.in/mumbai/accounting-finance/?page=1',
    ]

    def parse(self, response):
        pass
        # # promoted
        # for i in range(0, 50):
        #     tbody = response.xpath(".//*[@id='promotedAd']/tbody")
        #     href = tbody.xpath("tr["+str(i)+"]/td/table/tbody/tr[1]/td[2]/h3/a/@href").extract()
        #     date = response.xpath("tr["+str(i)+"]/td/table/tbody/tr[2]/td[1]/p/text()").extract()
        #     if len(href) > 0:
        #         print(href)
        #         href = self._rstrip(href)[0]
        #         # date = self._rstrip(date)[0]
        #         req = scrapy.Request(href, callback=self.parse_job_details)
        #         req.meta['url'] = href
        #         req.meta['premium'] = True
        #         yield req
        #
        # # normal
        # for i in range(0, 100):
        #     tbody = response.xpath(".//*[@id='offers_table']/tbody")
        #     href = tbody.xpath("tr["+str(i)+"]/td/table/tbody/tr[1]/td[2]/h3/a/@href").extract()
        #     date = tbody.xpath("tr["+str(i)+"]/td/table/tbody/tr[2]/td[1]/p/text()").extract()
        #     if len(href) > 0 and len(date) > 0:
        #         href = self._rstrip(href)[0]
        #         date = self._rstrip(date)[0]
        #
        #         if date.lower() == 'yesterday':
        #             req = scrapy.Request(href, callback=self.parse_job_details)
        #             req.meta['url'] = href
        #             yield req
        #
        # base_url = response.url.split('?')[0]
        # try:
        #     query_params = response.url.split('?')[1]
        #     current_page = query_params.split('page=')[1]
        #     next = int(current_page) + 1
        #     if str(current_page) == str(response.meta.get('previous_page_number', '')):
        #         return
        # except IndexError:
        #     # first page
        #     current_page = 1
        #     next = 2
        # finally:
        #     next_page = base_url + "?page=" + str(next)
        #     req = scrapy.Request(next_page, callback=self.parse)
        #     req.meta['previous_page_number'] = current_page
        #     yield req

    def parse_job_details(self, response):

        job_title = response.xpath(".//*[@id='offer_active']/div[4]/div[1]/div[1]/div[1]/h1/text()").extract_first()
        job_title = self._rstrip([job_title])

        salary = response.xpath(".//*[@id='offeractions']/div/div[1]/div[1]/strong/span/text()").extract()
        salary = self._join(salary).encode('utf-8')

        name = response.xpath(".//*[@id='offeractions']/div/div[1]/div[2]/div/p/span[1]/text()").extract_first().encode('utf-8')

        phone_no = response.xpath(".//*[@id='contact_methods']/li[3]/div[2]/strong[1]/text()").extract_first().encode('utf-8')

        jd = response.xpath(".//*[@id='textContent']/p/text()").extract()
        job_desc = self._join(jd)

        location = response.xpath(".//*[@id='offer_active']/div[4]/div[1]/div[1]/div[1]/p[1]/span/span[2]/strong/text()").extract()
        location = self._join(location)

        job = JobItem()
        job['url'] = response.meta['url']
        job['title'] = job_title
        job['location'] = location
        job['description'] = job_desc
        job['address'] = location
        job['telephone'] = phone_no
        job['recruiter_name'] = name
        job['premium'] = response.meta.get('premium', '')

        print("#"*15)
        print("Job title: {}".format(job_title))
        print("Name: {}".format(name))
        print("Salary: {}".format(salary))
        print("Phone No: {}".format(phone_no))
        print("Location: {}".format(location))
        print("JD: {}...".format(job_desc[:15]))
        print("URL: {}".format(job["url"]))

        print("#"*15)

        return job

    def _rstrip(self, l):
        return [x.strip().replace("\r\n,", "") for x in l]

    def _join(self, l, delimeter=' '):
        return delimeter.join(self._rstrip(l))  # to remove \r\n characters

