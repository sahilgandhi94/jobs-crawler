import json
import re

import scrapy

from crawler.items import SectorItem

BACK_OFFICE = [
    # ============== Careesma ==============
    "http://www.careesma.in/jobs?q=back+office&lc=Mumbai&pg=1",
    "http://www.careesma.in/jobs?q=back+office+executive&lc=Mumbai&pg=1",
    "http://www.careesma.in/jobs?q=back+office+assistant&lc=Mumbai&pg=1",
    "http://www.careesma.in/jobs?q=data+entry&lc=Mumbai&pg=1",
    "http://www.careesma.in/jobs?q=computer+operator&lc=Mumbai&pg=1",
    "http://www.careesma.in/jobs?q=office+admin&lc=Mumbai&pg=1",
    "http://www.careesma.in/jobs?q=receptionist&lc=Mumbai&pg=1",
    "http://www.careesma.in/jobs?q=backofficejobs&lc=Mumbai&pg=1",
    "http://www.careesma.in/jobs?q=backofficeexecutive&lc=Mumbai&pg=1",
    "http://www.careesma.in/jobs?q=backofficeassistant&lc=Mumbai&pg=1",
    "http://www.careesma.in/jobs?q=dataentry&lc=Mumbai&pg=1",
    "http://www.careesma.in/jobs?q=computeroperator&lc=Mumbai&pg=1",
    "http://www.careesma.in/jobs?q=officeadmin&lc=Mumbai&pg=1",

    # ============== BabaJobs ==============
    "http://www.babajob.com/Jobs-Receptionist-in-Mumbai-page-1",

    #  ============== ClickIndia ==============
    "http://mumbai.clickindia.com/qu.php?xd=back+office&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=back+office+executive&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=back+office+assistant&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=data+entry&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=computer+operator&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=office+admin&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=receptionist&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=backoffice&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=backofficeexecutive&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=backofficeassistant&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=dataentry&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=computeroperator&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=officeadmin&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=receptionist&page=1",

    #  ============== Indeed ==============
    "http://www.indeed.co.in/jobs?q=back+office&l=Mumbai,+Maharashtra&start=0",
    "http://www.indeed.co.in/jobs?q=back+office+executive&l=Mumbai,+Maharashtra&start=0",
    "http://www.indeed.co.in/jobs?q=back+office+assistant&l=Mumbai,+Maharashtra&start=0",
    "http://www.indeed.co.in/jobs?q=data+entry&l=Mumbai,+Maharashtra&start=0",
    "http://www.indeed.co.in/jobs?q=computer+operator&l=Mumbai,+Maharashtra&start=0",
    "http://www.indeed.co.in/jobs?q=office+admin&l=Mumbai,+Maharashtra&start=0",
    "http://www.indeed.co.in/jobs?q=receptionist&l=Mumbai,+Maharashtra&start=0",
    "http://www.indeed.co.in/jobs?q=backofficejobs&l=Mumbai,+Maharashtra&start=0",
    "http://www.indeed.co.in/jobs?q=backofficeexecutive&l=Mumbai,+Maharashtra&start=0",
    "http://www.indeed.co.in/jobs?q=backofficeassistant&l=Mumbai,+Maharashtra&start=0",
    "http://www.indeed.co.in/jobs?q=dataentry&l=Mumbai,+Maharashtra&start=0",
    "http://www.indeed.co.in/jobs?q=computeroperator&l=Mumbai,+Maharashtra&start=0",
    "http://www.indeed.co.in/jobs?q=officeadmin&l=Mumbai,+Maharashtra&start=0",
]

ACCOUNTS = [
    # ============== Careesma ==============
    "http://www.careesma.in/jobs?q=accounts&lc=Mumbai&pg=1",
    "http://www.careesma.in/jobs?q=account&lc=Mumbai&pg=1",
    "http://www.careesma.in/jobs?q=accountant&lc=Mumbai&pg=1",
    "http://www.careesma.in/jobs?q=accounts+assistant&lc=Mumbai&pg=1",
    "http://www.careesma.in/jobs?q=account+assistant&lc=Mumbai&pg=1",
    "http://www.careesma.in/jobs?q=book+keeping&lc=Mumbai&pg=1",
    "http://www.careesma.in/jobs?q=accounting&lc=Mumbai&pg=1",
    "http://www.careesma.in/jobs?q=finance&lc=Mumbai&pg=1",
    "http://www.careesma.in/jobs?q=finance+and+accounts&lc=Mumbai&pg=1",
    "http://www.careesma.in/jobs?q=accountsassistant&lc=Mumbai&pg=1",
    "http://www.careesma.in/jobs?q=accountassistant&lc=Mumbai&pg=1",
    "http://www.careesma.in/jobs?q=bookkeeping&lc=Mumbai&pg=1",
    "http://www.careesma.in/jobs?q=accountingjobs+in+mumbai",
    "http://www.careesma.in/jobs?q=financejobs+in+mumbai",
    "http://www.careesma.in/jobs?q=financeandaccounts&lc=Mumbai&pg=1",

    # ============== Baba jobs ==============
    "http://www.babajob.com/Jobs-Accountant-in-Mumbai-page-1",

    # ============== ClickIndia ==============
    "http://mumbai.clickindia.com/qu.php?xd=accounts&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=accountant&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=account&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=accounts+assistant&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=account+assistant&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=book+keeping&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=accounting&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=finance&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=finance+and+accounts&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=accountsassistant&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=accountassistant&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=bookkeeping&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=financeandaccounts&page=1",

    # ============== Indeed ==============
    "http://www.indeed.co.in/jobs?q=accounts&l=Mumbai,+Maharashtra&start=0",
    "http://www.indeed.co.in/jobs?q=account&l=Mumbai,+Maharashtra&start=0",
    "http://www.indeed.co.in/jobs?q=accountant&l=Mumbai,+Maharashtra&start=0",
    "http://www.indeed.co.in/jobs?q=accounts+assistant&l=Mumbai,+Maharashtra&start=0",
    "http://www.indeed.co.in/jobs?q=account+assistant&l=Mumbai,+Maharashtra&start=0",
    "http://www.indeed.co.in/jobs?q=book+keeping&l=Mumbai,+Maharashtra&start=0",
    "http://www.indeed.co.in/jobs?q=accounting&l=Mumbai,+Maharashtra&start=0",
    "http://www.indeed.co.in/jobs?q=finance&l=Mumbai,+Maharashtra&start=0",
    "http://www.indeed.co.in/jobs?q=finance+and+accounts&l=Mumbai,+Maharashtra&start=0",
    "http://www.indeed.co.in/jobs?q=accountsassistant&l=Mumbai,+Maharashtra&start=0",
    "http://www.indeed.co.in/jobs?q=accountassistant&l=Mumbai,+Maharashtra&start=0",
    "http://www.indeed.co.in/jobs?q=bookkeeping&l=Mumbai,+Maharashtra&start=0",
    "http://www.indeed.co.in/jobs?q=accountingjobs+in+mumbai",
    "http://www.indeed.co.in/jobs?q=financejobs+in+mumbai",
    "http://www.indeed.co.in/jobs?q=financeandaccounts&l=Mumbai,+Maharashtra&start=0",
]

OFFICE_BOY = [
    # ============== Careesma ==============
    "http://www.careesma.in/jobs?q=poen&lc=Mumbai&pg=1",
    "http://www.careesma.in/jobs?q=office+boy&lc=Mumbai&pg=1",
    "http://www.careesma.in/jobs?q=office+boy+assistant&lc=Mumbai&pg=1",
    "http://www.careesma.in/jobs?q=helper&lc=Mumbai&pg=1"
    "http://www.careesma.in/jobs?q=officeboy&lc=Mumbai&pg=1",
    "http://www.careesma.in/jobs?q=officeboyassistant&lc=Mumbai&pg=1",

    # ============== Babajob ==============
    "http://www.babajob.com/Jobs-Helper-in-Mumbai-page-1",

    # ============== Click India ==============
    "http://mumbai.clickindia.com/qu.php?xd=poen&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=office+boy&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=office+assistant&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=helper&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=officeboy&page=1",
    "http://mumbai.clickindia.com/qu.php?xd=officeassistant&page=1",

    # ============== Indeed ==============
    "http://www.indeed.co.in/jobs?q=poen&l=Mumbai,+Maharashtra&start=0",
    "http://www.indeed.co.in/jobs?q=office+boy&l=Mumbai,+Maharashtra&start=0",
    "http://www.indeed.co.in/jobs?q=office+boy+assistant&l=Mumbai,+Maharashtra&start=0",
    "http://www.indeed.co.in/jobs?q=helper&l=Mumbai,+Maharashtra&start=0"
    "http://www.indeed.co.in/jobs?q=officeboy&l=Mumbai,+Maharashtra&start=0",
    "http://www.indeed.co.in/jobs?q=officeboyassistant&l=Mumbai,+Maharashtra&start=0",
]


class Sector1Spider(scrapy.Spider):
    name = "sector1"
    allowed_domains = ["careesma.in", "babajob.com", "indeed.co.in", "clickindia.com", "click.in"]
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
        if "careesma.in" in response.url:
            return self.parse_careesma(response)
        elif "babajob.com" in response.url:
            return self.parse_baba(response)
        elif "clickindia.com" in response.url:
            return self.parse_clickindia(response)
        elif "indeed.co.in" in response.url:
            return self.parse_indeed(response)
        else:
            return

    def parse_careesma(self, response):

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
            url = rx(".//*[@id='results-list']/li["+str(i)+"]/div[1]/div[1]/div/h2/a/@href").extract_first()
            if url is not None:
                url = "http://careesma.in"+url
                date = rx(".//*[@id='results-list']/li["+str(i)+"]/div[1]/div[2]/span//text()").extract_first()
                if date is not None:
                    date = date.encode('utf-8')
                    if date.strip().lower().find('yesterday') > -1:
                        req = scrapy.Request(url, callback=self.parse_careesma_details)
                        req.meta['position'] = position
                        yield req

        # pagination
        curret_page = response.url.split('pg=')[1]
        next_page = int(curret_page) + 1
        if next_page < 100:
            next_url = response.url.split('pg=')[0] + "pg=" + str(next_page)
            req = scrapy.Request(next_url, callback=self.parse_careesma)
            req.meta['position'] = position
            yield req

    def parse_careesma_details(self, response):
        rx = response.xpath
        job = SectorItem()
        job['title'] = rx(".//*[@id='job-head']/div[1]/h1//text()").extract()
        job['date'] = 'yesterday'
        job['description'] = self._join(rx(".//*[@id='page-content']/div/div[2]/div[1]/div[2]/p/text()").extract())
        # index = job['description'].rfind('Location')
        # job['location'] = job['description'][index:]
        job['location'] = rx(".//*[@id='job-head']/div[1]/h2//text()").extract_first()
        job['company_name'] = rx(".//*[@id='job-head']/div[1]/h3/a/span/text()").extract_first()
        # contact_person_name = ''
        # job['role'] = rx(".//*[@id='page-content']/div/div[2]/div[1]/div[5]/dl[2]/dd[4]/text()").extract_first()
        job['position'] = response.meta.get('position', '')
        job['portal'] = 'careesma'

        yield job

    def parse_baba(self, response):
        if response.url in BACK_OFFICE:
            position = 'back_office'
        elif response.url in ACCOUNTS:
            position = 'accounts'
        elif response.url in OFFICE_BOY:
            position = 'office_boy'
        else:
            position = response.meta.get('position', '')
        i = 0
        if response.xpath('//div[@itemtype="http://schema.org/JobPosting"]').extract_first() is not None:
            for each in response.xpath('//div[@itemtype="http://schema.org/JobPosting"]'):
                href = each.xpath('div/div/h2[@class="s-card-title"]/a/@href')
                url = response.urljoin(href.extract_first())
                posted_date_string = each.xpath('div[1]/div[2]/ul/li[2]/p[2]/text()').extract_first()
                i += 1
                if posted_date_string is not None and posted_date_string.rfind('hours') > -1:
                    _l_path = ".//*[@id='searchResults']/div[2]/div["+str(i)+"]/div/div[1]/div[1]/div/ul/li[1]/text()"
                    print(_l_path)
                    location = response.xpath(_l_path).extract_first()
                    if location is None:
                        location = ''
                    req = scrapy.Request(url, callback=self.parse_baba_details)
                    req.meta['url'] = url
                    req.meta['location'] = json.dumps(location)
                    yield req

            url = response.url
            page_count = url.split('page-')[1]
            page_count = int(page_count)
            next_page = str(page_count + 1)
            nextUrl = url.split('page-')[0] + "page-" + next_page
            paginate_req = scrapy.Request(nextUrl, callback=self.parse_baba)
            paginate_req.meta['position'] = position
            yield paginate_req

    def parse_baba_details(self, response):
        job = SectorItem()
        rx = response.xpath
        job['date'] = 'yesterday'
        job['title'] = rx('//div[@class="row"]/div[@class="col-sm-12"]/h1/text()').extract_first()
        job['location'] = response.meta.get('location', '')
        job['description'] = rx('//div[div[@class="col-sm-2 job-label-text"]/img[@alt="Description"]]/div[@class="col-sm-10 job-info-text"]/text()').extract_first()
        job['company_name'] = rx('//div[div[@class="col-sm-2 job-label-text"]/img[@alt="Employer picture"]]/div[@class="col-sm-10 job-info-text"]/text()').extract_first()
        # job['contact_person_name'] =
        # job['number'] = phone_no
        job['portal'] = 'babajobs'
        job['position'] = response.meta.get('position', '')

        yield job

    def parse_clickindia(self, response):
        if response.url in BACK_OFFICE:
            position = 'back_office'
        elif response.url in ACCOUNTS:
            position = 'accounts'
        elif response.url in OFFICE_BOY:
            position = 'office_boy'
        else:
            position = response.meta.get('position', '')

        rx = response.xpath
        for i in range(1, 100):
            url = rx(".//*[@id='updates']/div["+str(i)+"]/a[1]/@href").extract_first()
            if url is not None:
                title = rx(".//*[@id='updates']/div["+str(i)+"]/a[1]/div[2]/span[2]//text()").extract()
                description = rx(".//*[@id='updates']/div["+str(i)+"]/a[1]/div[2]/div[3]/text()").extract()

                phone = rx(".//*[@id='updates']/div["+str(i)+"]/a[1]/div[2]/span[1]/text()").extract_first()
                date = rx(".//*[@id='updates']/div["+str(i)+"]/a[1]/div[2]/div[4]/span[1]/text()").extract_first()
                if date is not None:
                    date = date.split('-')[1].strip()

                req = scrapy.Request(url, callback=self.parse_clickindia_details)
                req.meta['position'] = position
                req.meta['title'] = json.dumps(title)
                req.meta['description'] = json.dumps(description)
                req.meta['phone'] = json.dumps(phone)
                req.meta['date'] = json.dumps(date)
                yield req

        url = response.url
        page_no = url.split('page=')[1]
        next_page = int(page_no)+1
        if next_page < 10:
            next_url = url.split('page=')[0] + 'page=' + str(next_page)
            req = scrapy.Request(next_url, callback=self.parse_clickindia)
            req.meta['position'] = position
            yield req

    def parse_clickindia_details(self, response):
        job = SectorItem()
        rx = response.xpath
        company_name = rx("//html/body/div[2]/div[2]/div[1]/div[1]/div[6]/span[2]//text()").extract()
        desc = rx("//html/body/div[2]/div[2]/div[1]/div[2]//text()").extract()
        location = rx("//html/body/div[2]/div[1]/h1/span/text()").extract()
        contact_person_name = rx(".//*[@id='detail_reply_box']/div/div[1]/div[1]//text()").extract()

        job['date'] = response.meta.get('date', '')
        job['title'] = response.meta.get('title', '')
        job['location'] = location
        job['description'] = response.meta.get('description', '') + " " + json.dumps(desc)
        job['company_name'] = company_name
        job['contact_person_name'] = contact_person_name
        job['number'] = response.meta.get('phone', '')
        job['position'] = response.meta.get('position', '')
        job['portal'] = 'clickindia'

        yield job

    def parse_clickin(self, response):
        rx = response.xpath
        url = rx(".//*[@class='clickin-listingpagePostsNoImage']/div[1]/div[1]/a")
        date = rx(".//*[@class='clickin-listingpagePostsNoImage']/div[1]/div[4]/span//text()")
        pass

    def parse_clickin_details(self, response):

        rx = response.xpath
        title = rx(".//*[@id='clickin-leftPanelInner-top']/div[3]/div[1]/div[1]/header/h1//text()").extract()
        role = rx(".//*[@id='clickin-leftPanelInner-top']/div[3]/div[1]/div[1]/div[4]/div/ul/li[1]/b//text()").extract()
        location = rx(".//*[@id='clickin-leftPanelInner-top']/div[3]/div[1]/div[1]/div[4]/div/ul/li[4]/b//text()").extract()
        desc = rx(".//*[@id='clickin-leftPanelInner-top']/div[3]/div[1]/p//text()").extract()
        company = rx(".//*[@id='clickin-leftPanelInner-top']/div[3]/div[1]/div[1]/div[4]/div/ul/li[7]/b//text()").extract()
        number = rx(".//*[@id='clickin-leftPanelInner-top']/div[3]/div[1]/div[1]/div[5]/div[1]/div[1]//text()").extract()
        pass

    def parse_indeed(self, response):
        if response.url in BACK_OFFICE:
            position = 'back_office'
        elif response.url in ACCOUNTS:
            position = 'accounts'
        elif response.url in OFFICE_BOY:
            position = 'office_boy'
        else:
            position = response.meta.get('position', '')

        rx = response.xpath
        if rx("//div[@itemtype='http://schema.org/JobPosting']").extract_first() is not None:
            i = 0
            for each in rx("//div[@itemtype='http://schema.org/JobPosting']"):
                i += 1
                title = self._join(each.xpath("h2//text()").extract())
                url = self._join(each.xpath("h2/a/@href").extract())
                company_name = self._join(each.xpath("span[1]/span/text()").extract())
                location = self._join(each.xpath("span[2]/span/span/text()").extract())
                date = self._join(each.xpath("table/*/*/*/*/span[@class='date']//text()").extract())

                if date.rfind('1 day ago') > -1:
                    print("YAY!")
                    url = 'http://www.indeed.co.in' + url
                    req = scrapy.Request(url, callback=self.parse_indeed_details)
                    req.meta['title'] = title
                    req.meta['company_name'] = company_name
                    req.meta['location'] = location
                    req.meta['date'] = date
                    req.meta['position'] = position
                    yield req

            if 'next' in response.xpath('.//div[@class="pagination"]/a//text()').extract()[-1].lower():
                next = response.xpath('.//div[@class="pagination"]/a/@href').extract()[-1]
                if 'indeed.co.in' not in next:
                    next = 'http://indeed.co.in' + next
                print("FOUND NEXT!! {}".format(next))
                paginate_req = scrapy.Request(next, callback=self.parse_indeed)
                paginate_req.meta['position'] = position
                yield paginate_req

    def parse_indeed_details(self, response):

        job = SectorItem()
        job['date'] = response.meta.get('date', '')
        job['title'] = response.meta.get('title', '')
        job['location'] = response.meta.get('location', '')
        job['description'] = self._join(response.xpath(".//*[@id='job_summary']//text()").extract())
        job['company_name'] = response.meta.get('company_name', '')
        # job['contact_person_name'] = response.meta.get('company_name', '')
        job['number'] = ', '.join(re.findall(r'[0-9]{10}|[0-9]{8}', job['description']))
        job['position'] = response.meta.get('position', '')
        job['portal'] = 'indeed'
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
