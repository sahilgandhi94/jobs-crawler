"""
This spider does a full text search for phone nos and email address in all the pages crawled.
Phone regex: \b[789]\d{2}[-.]?\d{3}[-.]?\d{4}\b
Email regex: [\w\.]+@[\w\.]+\.\w+

"""
import re
import traceback

from boto3.session import Session

import scrapy


class LeadsItem(scrapy.Item):
    number = scrapy.Field()
    email = scrapy.Field()
    company_name = scrapy.Field()


class Sector1Spider(scrapy.Spider):
    name = "phoneandemail"
    allowed_domains = ["timesjobs.com", "needjobsoon.com"]
    start_urls = [
        # ==================== TimesJobs ====================
        "http://www.timesjobs.com/candidate/job-search.html?from=submit&actualTxtKeywords=Back%20Office&searchBy=0&rdoOperator=OR&searchType=personalizedSearch&txtLocation=Mumbai&luceneResultSize=25&postWeek=60&txtKeywords=0DQT0back%20office0DQT0&pDate=I&sequence=1&startPage=1",
        "http://www.timesjobs.com/candidate/job-search.html?from=submit&actualTxtKeywords=Back%20Office%20Executive&searchBy=0&rdoOperator=OR&searchType=personalizedSearch&txtLocation=Mumbai&luceneResultSize=25&postWeek=60&txtKeywords=0DQT0back%20office0DQT0&pDate=I&sequence=1&startPage=1",
        "http://www.timesjobs.com/candidate/job-search.html?from=submit&actualTxtKeywords=Back%20Office%20Assistant&searchBy=0&rdoOperator=OR&searchType=personalizedSearch&txtLocation=Mumbai&luceneResultSize=25&postWeek=60&txtKeywords=0DQT0back%20office0DQT0&pDate=I&sequence=1&startPage=1",
        "http://www.timesjobs.com/candidate/job-search.html?from=submit&actualTxtKeywords=Back%20Entry&searchBy=0&rdoOperator=OR&searchType=personalizedSearch&txtLocation=Mumbai&luceneResultSize=25&postWeek=60&txtKeywords=0DQT0back%20office0DQT0&pDate=I&sequence=1&startPage=1",
        "http://www.timesjobs.com/candidate/job-search.html?from=submit&actualTxtKeywords=Computer%20Operator&searchBy=0&rdoOperator=OR&searchType=personalizedSearch&txtLocation=Mumbai&luceneResultSize=25&postWeek=60&txtKeywords=0DQT0back%20office0DQT0&pDate=I&sequence=1&startPage=1",
        "http://www.timesjobs.com/candidate/job-search.html?from=submit&actualTxtKeywords=Office%20Admin&searchBy=0&rdoOperator=OR&searchType=personalizedSearch&txtLocation=Mumbai&luceneResultSize=25&postWeek=60&txtKeywords=0DQT0back%20office0DQT0&pDate=I&sequence=1&startPage=1",
        "http://www.timesjobs.com/candidate/job-search.html?from=submit&actualTxtKeywords=Receptionist&searchBy=0&rdoOperator=OR&searchType=personalizedSearch&txtLocation=Mumbai&luceneResultSize=25&postWeek=60&txtKeywords=0DQT0back%20office0DQT0&pDate=I&sequence=1&startPage=1",

        "http://www.timesjobs.com/candidate/job-search.html?from=submit&actualTxtKeywords=Poen&searchBy=0&rdoOperator=OR&searchType=personalizedSearch&txtLocation=Mumbai&luceneResultSize=25&postWeek=60&txtKeywords=0DQT0back%20office0DQT0&pDate=I&sequence=1&startPage=1",
        "http://www.timesjobs.com/candidate/job-search.html?from=submit&actualTxtKeywords=Office%20Boy&searchBy=0&rdoOperator=OR&searchType=personalizedSearch&txtLocation=Mumbai&luceneResultSize=25&postWeek=60&txtKeywords=0DQT0back%20office0DQT0&pDate=I&sequence=1&startPage=1",
        "http://www.timesjobs.com/candidate/job-search.html?from=submit&actualTxtKeywords=Office%20Assistant&searchBy=0&rdoOperator=OR&searchType=personalizedSearch&txtLocation=Mumbai&luceneResultSize=25&postWeek=60&txtKeywords=0DQT0back%20office0DQT0&pDate=I&sequence=1&startPage=1",
        "http://www.timesjobs.com/candidate/job-search.html?from=submit&actualTxtKeywords=Helper&searchBy=0&rdoOperator=OR&searchType=personalizedSearch&txtLocation=Mumbai&luceneResultSize=25&postWeek=60&txtKeywords=0DQT0back%20office0DQT0&pDate=I&sequence=1&startPage=1",

        "http://www.timesjobs.com/candidate/job-search.html?from=submit&actualTxtKeywords=Accounts&searchBy=0&rdoOperator=OR&searchType=personalizedSearch&txtLocation=Mumbai&luceneResultSize=25&postWeek=60&txtKeywords=0DQT0back%20office0DQT0&pDate=I&sequence=1&startPage=1",
        "http://www.timesjobs.com/candidate/job-search.html?from=submit&actualTxtKeywords=Accountant&searchBy=0&rdoOperator=OR&searchType=personalizedSearch&txtLocation=Mumbai&luceneResultSize=25&postWeek=60&txtKeywords=0DQT0back%20office0DQT0&pDate=I&sequence=1&startPage=1",
        "http://www.timesjobs.com/candidate/job-search.html?from=submit&actualTxtKeywords=Account&searchBy=0&rdoOperator=OR&searchType=personalizedSearch&txtLocation=Mumbai&luceneResultSize=25&postWeek=60&txtKeywords=0DQT0back%20office0DQT0&pDate=I&sequence=1&startPage=1",
        "http://www.timesjobs.com/candidate/job-search.html?from=submit&actualTxtKeywords=Accounts%20Assistant&searchBy=0&rdoOperator=OR&searchType=personalizedSearch&txtLocation=Mumbai&luceneResultSize=25&postWeek=60&txtKeywords=0DQT0back%20office0DQT0&pDate=I&sequence=1&startPage=1",
        "http://www.timesjobs.com/candidate/job-search.html?from=submit&actualTxtKeywords=Book%20Keeping&searchBy=0&rdoOperator=OR&searchType=personalizedSearch&txtLocation=Mumbai&luceneResultSize=25&postWeek=60&txtKeywords=0DQT0back%20office0DQT0&pDate=I&sequence=1&startPage=1",
        "http://www.timesjobs.com/candidate/job-search.html?from=submit&actualTxtKeywords=Accounting&searchBy=0&rdoOperator=OR&searchType=personalizedSearch&txtLocation=Mumbai&luceneResultSize=25&postWeek=60&txtKeywords=0DQT0back%20office0DQT0&pDate=I&sequence=1&startPage=1",
        "http://www.timesjobs.com/candidate/job-search.html?from=submit&actualTxtKeywords=Finance%20and%20Accounts&searchBy=0&rdoOperator=OR&searchType=personalizedSearch&txtLocation=Mumbai&luceneResultSize=25&postWeek=60&txtKeywords=0DQT0back%20office0DQT0&pDate=I&sequence=1&startPage=1",


        # ==================== NeedJobSoon ====================
        "http://www.needjobsoon.com/jobs/Back%20Office/mumbai?page=0",
        "http://www.needjobsoon.com/jobs/Back%20Office%20Executive/mumbai?page=0",
        "http://www.needjobsoon.com/jobs/Back%20Office%20Assistant/mumbai?page=0",
        "http://www.needjobsoon.com/jobs/Back%20Entry/mumbai?page=0",
        "http://www.needjobsoon.com/jobs/Computer%20Operator/mumbai?page=0",
        "http://www.needjobsoon.com/jobs/Office%20Admin/mumbai?page=0",
        "http://www.needjobsoon.com/jobs/Receptionist/mumbai?page=0",

        "http://www.needjobsoon.com/jobs/Poen/mumbai?page=0",
        "http://www.needjobsoon.com/jobs/Office%20Boy/mumbai?page=0",
        "http://www.needjobsoon.com/jobs/Office%20Assistant/mumbai?page=0",
        "http://www.needjobsoon.com/jobs/Helper/mumbai?page=0",

        "http://www.needjobsoon.com/jobs/Accounts/mumbai?page=0",
        "http://www.needjobsoon.com/jobs/Accountant/mumbai?page=0",
        "http://www.needjobsoon.com/jobs/Account/mumbai?page=0",
        "http://www.needjobsoon.com/jobs/Accounts%20Assistant/mumbai?page=0",
        "http://www.needjobsoon.com/jobs/Book%20Keeping/mumbai?page=0",
        "http://www.needjobsoon.com/jobs/Accounting/mumbai?page=0",
        "http://www.needjobsoon.com/jobs/Finance%20and%20Accounts/mumbai?page=0",


    ]

    def parse(self, response):
        if "timesjob" in response.url:
            all_links = response.xpath('*//a/@href').extract()
            for link in all_links:
                if 'JobDetailView.html'.lower() in link.lower():
                    url = response.urljoin(link)
                    yield scrapy.Request(url, callback=self.parse_details)

            current = int(response.url.split('&startPage=')[1])
            if current < 50:
                next_params = "&sequence={}&startPage={}".format(current+1, current+1)
                next = response.url.split('&sequence=')[0] + next_params
                yield scrapy.Request(next, callback=self.parse)

        elif "needjobsoon" in response.url:
            all_links = response.xpath('*//a/@href').extract()
            for link in all_links:
                if 'needjobsoon.com/job/'.lower() in link.lower():
                    url = response.urljoin(link)
                    yield scrapy.Request(url, callback=self.parse_details)

            next = response.xpath('//li[@class="pager-next"]/a/@href').extract_first()
            next = response.urljoin(next)
            print(next)
            yield scrapy.Request(next, callback=self.parse)
        else:
            return

    def parse_details(self, response):
        if "timesjob" in response.url:
            company_name = response.xpath("//*[@class=\"jd-company-name\"]//text()").extract_first()
        elif "needjobsoon" in response.url:
            company_name = re.findall(r"\bPosted By\b : (.)+", response.body)
            company_name = ' '.join(company_name)
        else:
            company_name = 'NA'
        number = ','.join(list(set(re.findall(r"\b[789]\d{2}[-.]?\d{3}[-.]?\d{4}\b", response.body))))
        email = ','.join(list(set(re.findall(r"[\w\.]+@[\w\.]+\.\w+", response.body))))

        if company_name is not None:
            company_name.strip()

        dynamodb_session = Session(aws_access_key_id='AKIAJT6AN3A5WZEZ74WA',
                                   aws_secret_access_key='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',
                                   region_name="us-east-1")
        dynamodb = dynamodb_session.resource('dynamodb', region_name='us-east-1')
        try:
            try:
                phone = number
                if not (phone is None or len(phone) < 8):
                    print("Uploading phone no {}".format(number))
                    company_name = 'NA' if company_name is None else company_name
                    table = dynamodb.Table('phone_no_leads')
                    table.put_item(Item={'phone': phone, 'company': company_name})
            except KeyError:
                print("Suppressed error {}".format(traceback.format_exc()))

            try:
                if not(email is None or len(email) < 4):
                    print("Uploading email {}".format(email))
                    company_name = 'NA' if company_name is None else company_name
                    table = dynamodb.Table('email_leads')
                    table.put_item(Item={'email': email, 'company': company_name})
            except KeyError:
                print("Suppressed error {}".format(traceback.format_exc()))
        except:
            print("Suppressed error {}".format(traceback.format_exc()))

        return {
            'company_name': company_name,
            'phone_no': number,
            'email': email
        }

