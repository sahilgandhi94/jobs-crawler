import re
from datetime import datetime, timedelta

import scrapy
import requests

from crawler.items import SectorItem

BACK_OFFICE = [
    # ============== Naukri ==============
    "http://www.naukri.com/back-office-jobs-in-mumbai",
    "http://www.naukri.com/back-office-executive-jobs-in-mumbai",
    "http://www.naukri.com/back-office-assistant-jobs-in-mumbai",
    "http://www.naukri.com/data-entry-jobs-in-mumbai",
    "http://www.naukri.com/computer-operator-jobs-in-mumbai",
    "http://www.naukri.com/office-admin-jobs-in-mumbai",
    "http://www.naukri.com/receptionist-jobs-in-mumbaibackofficejobs-in-mumbai",
    "http://www.naukri.com/backofficeexecutive-jobs-in-mumbai",
    "http://www.naukri.com/backofficeassistant-jobs-in-mumbai",
    "http://www.naukri.com/dataentry-jobs-in-mumbai",
    "http://www.naukri.com/computeroperator-jobs-in-mumbai",
    "http://www.naukri.com/officeadmin-jobs-in-mumbai",

    # ============== Olx ==============
    "https://www.olx.in/mumbai/jobs/q-back-office/",
    "https://www.olx.in/mumbai/jobs/q-back-office-executive/",
    "https://www.olx.in/mumbai/jobs/q-back-office-assistant/",
    "https://www.olx.in/mumbai/jobs/q-data-entry/",
    "https://www.olx.in/mumbai/jobs/q-computer-operator/",
    "https://www.olx.in/mumbai/jobs/q-office-admin/",
    "https://www.olx.in/mumbai/jobs/q-receptionist/",
    "https://www.olx.in/mumbai/jobs/q-backoffice/",
    "https://www.olx.in/mumbai/jobs/q-backofficeexecutive/",
    "https://www.olx.in/mumbai/jobs/q-backofficeassistant/",
    "https://www.olx.in/mumbai/jobs/q-dataentry/",
    "https://www.olx.in/mumbai/jobs/q-computeroperator/",
    "https://www.olx.in/mumbai/jobs/q-officeadmin/",

    # ============== Shine ==============
    "http://www.shine.com/job-search/simple/back-office/mumbai/",
    "http://www.shine.com/job-search/simple/back-office-executive/mumbai/",
    "http://www.shine.com/job-search/simple/back-office-assistant/mumbai/",
    "http://www.shine.com/job-search/simple/data-entry/mumbai/",
    "http://www.shine.com/job-search/simple/computer-operator/mumbai/",
    "http://www.shine.com/job-search/simple/office-admin/mumbai/",
    "http://www.shine.com/job-search/simple/receptionist/mumbai/",
]

ACCOUNTS = [
    # ============== Naukri ==============
    "http://www.naukri.com/accounts-jobs-in-mumbai",
    "http://www.naukri.com/account-jobs-in-mumbai",
    "http://www.naukri.com/accountant-jobs-in-mumbai",
    "http://www.naukri.com/accounts-assistant-jobs-in-mumbai",
    "http://www.naukri.com/account-assistant-jobs-in-mumbai",
    "http://www.naukri.com/book-keeping-jobs-in-mumbai",
    "http://www.naukri.com/accounting-jobs-in-mumbai",
    "http://www.naukri.com/finance-jobs-in-mumbai",
    "http://www.naukri.com/finance-and-accounts-jobs-in-mumbai",
    "http://www.naukri.com/accountsassistant-jobs-in-mumbai",
    "http://www.naukri.com/accountassistant-jobs-in-mumbai",
    "http://www.naukri.com/bookkeeping-jobs-in-mumbai",
    "http://www.naukri.com/accountingjobs-in-mumbai",
    "http://www.naukri.com/financejobs-in-mumbai",
    "http://www.naukri.com/financeandaccounts-jobs-in-mumbai",

    # ============== Olx ==============
    "https://www.olx.in/mumbai/jobs/q-accounts/",
    "https://www.olx.in/mumbai/jobs/q-accountant/",
    "https://www.olx.in/mumbai/jobs/q-account/",
    "https://www.olx.in/mumbai/jobs/q-accounts-assistant/",
    "https://www.olx.in/mumbai/jobs/q-account-assistant/",
    "https://www.olx.in/mumbai/jobs/q-book-keeping/",
    "https://www.olx.in/mumbai/jobs/q-accounting/",
    "https://www.olx.in/mumbai/jobs/q-finance/",
    "https://www.olx.in/mumbai/jobs/q-finance-and-accounts/",
    "https://www.olx.in/mumbai/jobs/q-accountsassistant/",
    "https://www.olx.in/mumbai/jobs/q-accountassistant/",
    "https://www.olx.in/mumbai/jobs/q-bookkeeping/",
    "https://www.olx.in/mumbai/jobs/q-financeandaccounts/",

    # ============== Shine ==============
    "http://www.shine.com/job-search/simple/accounts/mumbai/",
    "http://www.shine.com/job-search/simple/accountant/mumbai/",
    "http://www.shine.com/job-search/simple/account/mumbai/",
    "http://www.shine.com/job-search/simple/accounts-assistant/mumbai/",
    "http://www.shine.com/job-search/simple/account-assistant/mumbai/",
    "http://www.shine.com/job-search/simple/book-keeping/mumbai/",
    "http://www.shine.com/job-search/simple/accounting/mumbai/",
    "http://www.shine.com/job-search/simple/finance/mumbai/",
    "http://www.shine.com/job-search/simple/finance-and-accounts/mumbai/",
    "http://www.shine.com/job-search/simple/accountsassistant/mumbai/",
    "http://www.shine.com/job-search/simple/accountassistant/mumbai/",
    "http://www.shine.com/job-search/simple/bookkeeping/mumbai/",
    "http://www.shine.com/job-search/simple/financeandaccounts/mumbai/",
]

OFFICE_BOY = [
    # ============== Naukri ==============
    "http://www.naukri.com/poen-jobs-in-mumbai",
    "http://www.naukri.com/office-boy-jobs-in-mumbai",
    "http://www.naukri.com/office-boy-assistant-jobs-in-mumbai",
    "http://www.naukri.com/helper-jobs-in-mumbai"
    "http://www.naukri.com/officeboy-jobs-in-mumbai",
    "http://www.naukri.com/officeboyassistant-jobs-in-mumbai",

    # ============== Olx ==============
    "https://www.olx.in/mumbai/jobs/q-poen/",
    "https://www.olx.in/mumbai/jobs/q-office-boy/",
    "https://www.olx.in/mumbai/jobs/q-office-assistant/",
    "https://www.olx.in/mumbai/jobs/q-helper/",
    "https://www.olx.in/mumbai/jobs/q-officeboy/",
    "https://www.olx.in/mumbai/jobs/q-officeassistant/",

    # ============== Shine ==============
    "http://www.shine.com/job-search/simple/poen/mumbai/",
    "http://www.shine.com/job-search/simple/office-boy/mumbai/",
    "http://www.shine.com/job-search/simple/office-assistant/mumbai/",
    "http://www.shine.com/job-search/simple/helper/mumbai/",
    "http://www.shine.com/job-search/simple/officeboy/mumbai/",
    "http://www.shine.com/job-search/simple/officeassistant/mumbai/",
]


class SectorSpider(scrapy.Spider):
    name = "sector"
    allowed_domains = ["naukri.com", "shine.com", "olx.in"]
    start_urls = []

    def __init__(self, sector=None, **kwargs):
        if sector is not None:
            sectors = sector.split(',')
            if "back_office" in sectors:
                self.start_urls += BACK_OFFICE
            elif "accounts" in sectors:
                self.start_urls += ACCOUNTS
            elif "office_boy" in sectors:
                self.start_urls += OFFICE_BOY
        else:
            self.start_urls = BACK_OFFICE + ACCOUNTS + OFFICE_BOY

    def parse(self, response):
        if "naukri.com" in response.url:
            return self.parse_naukri(response)
        elif "shine.com" in response.url:
            return self.parse_shine(response)
        elif "olx.in" in response.url:
            print('sending to olx parse')
            return self.parse_olx(response)
        else:
            return

    def parse_shine(self, response):

        if response.url in BACK_OFFICE:
            position = 'back_office'
        elif response.url in ACCOUNTS:
            position = 'accounts'
        elif response.url in OFFICE_BOY:
            position = 'office_boy'
        else:
            position = response.meta.get('position', '')

        rx = response.xpath
        for i in range(0, 50):
            # print(i)
            url = rx('.//*[@itemtype="http://schema.org/JobPosting"]['+str(i)+']/div[2]/a/@href').extract_first()
            date = rx('.//*[@itemtype="http://schema.org/JobPosting"]['+str(i)+']/div[3]/div[2]/text()').extract_first()
            if date is not None:
                # print("URL got {}".format(url))
                # print("Date got {}".format(date))
                posted_date_list = date.split()
                posted_date = datetime.strptime(posted_date_list[2], '%d-%b-%Y').date()
                # print("posted_date {}".format(posted_date))
                today = datetime.now().date()
                # print("Today {}".format(today))
                if (today - posted_date) <= timedelta(1):
                    # print("Scrapping......")
                    url = 'http://shine.com'+url
                    # print(url)
                    req = scrapy.Request(url, callback=self.parse_shine_details)
                    req.meta['position'] = position
                    req.meta['url'] = url
                    yield req

        nextUrl = ""
        flag = False
        try:
            # if int(response.meta['total_items_iterated']) <= int(response.meta['total_count']):
            url = response.url
            page_count = re.findall(r'\d+', url)[0]
            page_count = int(page_count)
            print("current page count {}".format(page_count))
            next_page = page_count + 1
            if next_page > 100:
                return  # adding this, because the total-items-iterated logic fails miserably
            nextUrl = re.sub(r'\d+', str(next_page), url)
            print("Next url {}".format(nextUrl))
            flag = True
            # total_items_iterated = int(response.meta['total_items_iterated']) + count_per_page
        except:
            #first page
            flag = True
            nextUrl = response.url + str(2) + "/"
            # total_items_iterated = count_per_page
        finally:
            if not flag:
                return
            else:
                paginate_req = scrapy.Request(nextUrl, callback=self.parse_shine)
                paginate_req.meta['position'] = position
                # paginate_req.meta['total_count'] = total_count
                # paginate_req.meta['total_items_iterated'] = total_items_iterated
                yield paginate_req

    def parse_shine_details(self, response):
        # url = response.meta['url'].split('?')[0]
        if response.xpath('//div[@itemtype="http://schema.org/JobPosting"]').extract_first() is None:
            return
        job = SectorItem()
        job_posting = response.xpath('//div[@itemtype="http://schema.org/JobPosting"]')[0]
        job['title'] = job_posting.xpath('//h1[@itemprop="title"]/text()').extract_first()
        job['date'] = job_posting.xpath('//span[@itemprop="datePosted"]/text()').extract_first()
        # old location xpath: '//a[@class="normaljdsnippet jd_location curText cls_jd_primary_location"]//text()'
        job['location'] = self._join(job_posting.xpath(
            '//span[@itemtype="http://schema.org/Place"]/*//text()'
        ).extract(), delimeter=',')
        job['description'] = job_posting.xpath('//span[@itemprop="description"]//text()').extract()
        job['company_name'] = job_posting.xpath('//span[@itemprop="name"]/h2/text()').extract_first()
        job['contact_person_name'] = ''
        recruiter_details = job_posting.xpath('//div[@class="ropen cls_rect_detail_div"]/ul//text()').extract()
        for i in recruiter_details:
            try:
                phone = int(i.encode('utf-8').strip())
                job['number'] = phone
            except ValueError:
                pass

            try:
                if re.match('Email\s*(.*)', i):
                    job['email'] = recruiter_details[recruiter_details.index(i)+1]
            except:
                pass

        job['url'] = response.url
        job['role'] = self._join(job_posting.xpath('//span[@itemprop="skills"]/a/text()').extract(), delimeter=',')
        job['position'] = response.meta.get('position', '')
        job['portal'] = 'shine'

        yield job

    def parse_olx(self, response):
        print('in parse')
        if response.url in BACK_OFFICE:
            position = 'back_office'
        elif response.url in ACCOUNTS:
            position = 'accounts'
        elif response.url in OFFICE_BOY:
            position = 'office_boy'
        else:
            position = response.meta.get('position', '')
        print(response.url)
        print("POSITION {}".format(position))

        # promoted
        for i in range(0, 50):
            tbody = response.xpath(".//*[@id='promotedAd']/tbody")
            href = tbody.xpath("tr["+str(i)+"]/td/table/tbody/tr[1]/td[2]/h3/a/@href").extract()
            date = response.xpath("tr["+str(i)+"]/td/table/tbody/tr[2]/td[1]/p/text()").extract()
            if len(href) > 0:
                print(href)
                href = self._rstrip(href)[0]
                # date = self._rstrip(date)[0]
                req = scrapy.Request(href, callback=self.parse_olx_details)
                req.meta['url'] = href
                req.meta['premium'] = True
                req.meta['position'] = position
                yield req

        # normal
        for i in range(0, 100):
            tbody = response.xpath(".//*[@id='offers_table']/tbody")
            href = tbody.xpath("tr["+str(i)+"]/td/table/tbody/tr[1]/td[2]/h3/a/@href").extract()
            date = tbody.xpath("tr["+str(i)+"]/td/table/tbody/tr[2]/td[1]/p/text()").extract()
            if len(href) > 0 and len(date) > 0:
                href = self._rstrip(href)[0]
                date = self._rstrip(date)[0]

                if date.lower() == 'yesterday':
                    req = scrapy.Request(href, callback=self.parse_olx_details)
                    req.meta['url'] = href
                    req.meta['position'] = position
                    yield req

        base_url = response.url.split('?')[0]
        try:
            query_params = response.url.split('?')[1]
            current_page = query_params.split('page=')[1]
            next = int(current_page) + 1
            if str(current_page) == str(response.meta.get('previous_page_number', '')):
                return
        except IndexError:
            # first page
            current_page = 1
            next = 2
        finally:
            next_page = base_url + "?page=" + str(next)
            req = scrapy.Request(next_page, callback=self.parse_olx)
            req.meta['previous_page_number'] = current_page
            req.meta['position'] = position
            yield req

    def parse_olx_details(self, response):
        job_title = response.xpath(".//*[@id='offer_active']/div[4]/div[1]/div[1]/div[1]/h1/text()").extract_first()
        job_title = self._rstrip([job_title])

        salary = response.xpath(".//*[@id='offeractions']/div/div[1]/div[1]/strong/span/text()").extract()
        salary = self._join(salary).encode('utf-8')

        name = response.xpath(".//*[@id='offeractions']/div/div[1]/div[2]/div/p/span[1]/text()").extract_first()
        if name is not None:
            name = name.encode('utf-8')

        phone_no = response.xpath(".//*[@id='contact_methods']/li[3]/div[2]/strong[1]/text()").extract_first()
        if phone_no is not None:
            phone_no = phone_no.encode('utf-8')

        jd = response.xpath(".//*[@id='textContent']/p/text()").extract()
        job_desc = self._join(jd)

        location = response.xpath(".//*[@id='offer_active']/div[4]/div[1]/div[1]/div[1]/p[1]/span/span[2]/strong/text()").extract()
        location = self._join(location)

        job = SectorItem()
        job['url'] = response.url
        job['date'] = 'yesterday'
        job['title'] = job_title
        job['location'] = location
        job['description'] = job_desc
        job['company_name'] = ''
        job['contact_person_name'] = name
        job['number'] = phone_no
        job['portal'] = 'olx'
        job['position'] = response.meta.get('position', '')

        yield job

    def parse_naukri(self, response):
        if response.xpath('//div[@itemtype="http://schema.org/JobPosting"]/a/@href').extract_first() is not None:

            if response.url in BACK_OFFICE:
                position = 'back_office'
            elif response.url in ACCOUNTS:
                position = 'accounts'
            elif response.url in OFFICE_BOY:
                position = 'office_boy'
            else:
                position = response.meta.get('position', '')

            for each in response.xpath('//div[@itemtype="http://schema.org/JobPosting"]'):
                href = each.xpath('a/@href').extract_first()
                posted_date_string = each.xpath('div[@class="other_details"]'
                                                '/div[@class="rec_details"]/span[@class="date"]/text()').extract_first()
                url = response.urljoin(href)

                if posted_date_string == "1 day ago":
                    req = scrapy.Request(url, callback=self.parse_naukri_details)
                    req.meta['url'] = url
                    req.meta['position'] = position
                    yield req

            if response.xpath('//div[@class="pagination"]/a/@href').extract_first() is not None:
                next = response.xpath('//div[@class="pagination"]/a/@href').extract()
                if len(next) > 1:
                    next = response.urljoin(next[1])
                else:
                    next = response.urljoin(next[0])
                paginate_req = scrapy.Request(next, callback=self.parse_naukri)
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
                    paginate_req.meta['position'] = position
                    yield paginate_req

    def parse_naukri_details(self, response):
        # extract job-id
        url = response.meta['url'].split('?')[0]
        index = url.rfind('-')
        jid = url[index+1:]

        if response.xpath('//div[@itemtype="http://schema.org/JobPosting"]').extract_first() is None:
            return

        job = SectorItem()
        job_posting = response.xpath('//div[@itemtype="http://schema.org/JobPosting"]')[0]
        job['url'] = response.url
        job['title'] = self._join(job_posting.xpath('//h1[@itemprop="title"]/text()').extract())
        job['company_name'] = self._join(job_posting.xpath('//a[@itemprop="hiringOrganization"]/text()').extract())

        location = job_posting.xpath('//em[@itemprop="jobLocation"]')
        if location is not None:
            job['location'] = self._join(location[0].xpath('//div[@itemprop="name"]/a/text()').extract(), delimeter=', ')

        job['description'] = self._join(job_posting.xpath('//ul[@itemprop="description"]//text()').extract())
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
            contact = r.json()['fields']
            contact = {k.lower(): v for k, v in contact.items()}  # convert all keys to lowercase

            try:
                if 'email address' in contact.keys() and 'src' in contact['email address'].keys():
                    contact['email address'].pop('src', 'no src found')
                job['email'] = self._fetch(contact, 'email address', 'title')
            except:
                pass

            job['number'] = self._fetch(contact, 'telephone')
            # job['email'] = self._fetch(contact, 'email address', 'title')
            job['contact_person_name'] = self._fetch(contact, 'recruiter name')

        job['date'] = job_posting.xpath('//div[@class="sumFoot"]//text()').re('Posted\s*(.*)')
        job['portal'] = 'naukri'
        job['position'] = response.meta.get('position', '')
        yield job

    def _rstrip(self, l):
        return [x.strip().replace("\r\n,", "") for x in l]

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


