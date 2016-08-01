import time

import scrapy
import logging
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders.init import InitSpider
from scrapy.http import Request, FormRequest
from scrapy.spiders import Rule

from crawler.items  import CandidatescraperItem


class BabaJobSpider(InitSpider):
    name = "babajob_thane_other"
    allowed_domains=["babajob.com"]
    login_page = 'http://www.babajob.com/login'

    start_urls = [
         #"http://www.babajob.com/Hire-BPO-in-Thane-sort-dateDesc-in_last_days-2",
         #"http://www.babajob.com/Hire-Driver-in-Thane-sort-dateDesc-in_last_days-2",
         #"http://www.babajob.com/Hire-Helper-in-Thane-sort-dateDesc-in_last_days-2",
         #"http://www.babajob.com/Hire-Delivery-in-Thane-sort-dateDesc-in_last_days-2",
         #"http://www.babajob.com/Hire-Receptionist-in-Thane-sort-dateDesc-in_last_days-2",
         "http://www.babajob.com/Hire-Other-in-Thane-sort-dateDesc-in_last_days-2-include_mobile_users",
        # "http://www.babajob.com/Hire-DataEntry-in-Thane-sort-dateDesc-in_last_days-2",
        # "http://www.babajob.com/Hire-Cashier-in-Thane-sort-dateDesc-in_last_days-2",
        # "http://www.babajob.com/Hire-Sales-in-Thane-sort-dateDesc-in_last_days-2",
        # "http://www.babajob.com/Hire-Management-in-Thane-sort-dateDesc-in_last_days-2",
        # "http://www.babajob.com/Hire-Teacher-in-Thane-sort-dateDesc-in_last_days-2",
        # "http://www.babajob.com/Hire-Accountant-in-Thane-sort-dateDesc-in_last_days-2",
        # "http://www.babajob.com/Hire-Steward-in-Thane-sort-dateDesc-in_last_days-2",
        # # "http://www.babajob.com/Hire-Maid-in-Thane-sort-dateDesc-in_last_days-2",
        # # "http://www.babajob.com/Hire-Cook-in-Thane-sort-dateDesc-in_last_days-2",
        # # "http://www.babajob.com/Hire-Nanny-in-Thane-sort-dateDesc-in_last_days-2",
        # # "http://www.babajob.com/Hire-Guard-in-Thane-sort-dateDesc-in_last_days-2",
        # # "http://www.babajob.com/Hire-Laborer-in-Thane-sort-dateDesc-in_last_days-2",
        # # "http://www.babajob.com/Hire-Tailor-in-Thane-sort-dateDesc-in_last_days-2",
        # # "http://www.babajob.com/Hire-Nurse-in-Thane-sort-dateDesc-in_last_days-2",
        # # "http://www.babajob.com/Hire-Machinist-in-Thane-sort-dateDesc-in_last_days-2",
        # # "http://www.babajob.com/Hire-Engineer-in-Thane-sort-dateDesc-in_last_days-2",
        # # "http://www.babajob.com/Hire-Beautician-in-Thane-sort-dateDesc-in_last_days-2",
        #
         #"http://www.babajob.com/Hire-BPO-in-Mumbai-sort-dateDesc-in_last_days-2",
        # "http://www.babajob.com/Hire-Driver-in-Mumbai-sort-dateDesc-in_last_days-2",
         #"http://www.babajob.com/Hire-Helper-in-Mumbai-sort-dateDesc-in_last_days-2",
        # "http://www.babajob.com/Hire-Delivery-in-Mumbai-sort-dateDesc-in_last_days-2",
        # "http://www.babajob.com/Hire-Receptionist-in-Mumbai-sort-dateDesc-in_last_days-2",
        # "http://www.babajob.com/Hire-Other-in-Mumbai-sort-dateDesc-in_last_days-2",
        # "http://www.babajob.com/Hire-DataEntry-in-Mumbai-sort-dateDesc-in_last_days-2",
        # "http://www.babajob.com/Hire-Cashier-in-Mumbai-sort-dateDesc-in_last_days-2",
        # "http://www.babajob.com/Hire-Sales-in-Mumbai-sort-dateDesc-in_last_days-2",
        # "http://www.babajob.com/Hire-Management-in-Mumbai-sort-dateDesc-in_last_days-2",
        # "http://www.babajob.com/Hire-Teacher-in-Mumbai-sort-dateDesc-in_last_days-2",
        # "http://www.babajob.com/Hire-Accountant-in-Mumbai-sort-dateDesc-in_last_days-2",
        # "http://www.babajob.com/Hire-Steward-in-Mumbai-sort-dateDesc-in_last_days-2",
        # # "http://www.babajob.com/Hire-Maid-in-Mumbai-sort-dateDesc-in_last_days-2",
        # # "http://www.babajob.com/Hire-Cook-in-Mumbai-sort-dateDesc-in_last_days-2",
        # # "http://www.babajob.com/Hire-Nanny-in-Mumbai-sort-dateDesc-in_last_days-2",
        # # "http://www.babajob.com/Hire-Guard-in-Mumbai-sort-dateDesc-in_last_days-2",
        # # "http://www.babajob.com/Hire-Laborer-in-Mumbai-sort-dateDesc-in_last_days-2",
        # # "http://www.babajob.com/Hire-Tailor-in-Mumbai-sort-dateDesc-in_last_days-2",
        # # "http://www.babajob.com/Hire-Nurse-in-Mumbai-sort-dateDesc-in_last_days-2",
        # # "http://www.babajob.com/Hire-Machinist-in-Mumbai-sort-dateDesc-in_last_days-2",
        # # "http://www.babajob.com/Hire-Engineer-in-Mumbai-sort-dateDesc-in_last_days-2",
        # # "http://www.babajob.com/Hire-Beautician-in-Mumbai-sort-dateDesc-in_last_days-2",

    ]

    def init_request(self):
        print ("Init")
        self.download_delay = 15
        """This function is called before crawling starts."""
        return Request(url=self.login_page, callback=self.login)

    def login(self, response):
        print ("Login")
        """Generate a login request."""
        return FormRequest.from_response(response,
                    formdata={'LoginText': '7303038426', 'Password': 'Nishit123'},
                    clickdata={'id':'updateprofile-submit'},
                    callback=self.check_login_response)

    def check_login_response(self, response):
        print ("CheckLogin")
        if "Nishit" in response.body:
            # Now the crawling can begin..
            print ("Logged in")
            return self.initialized()
        else:
            print ("Login Failed")
            # Something went wrong, we couldn't log in, so nothing happens.

    def parse(self, response):
        print ("Parse")

        next = response.xpath('//a[@id="cp1_pagerSeekersGrid_btnNext"]/@href')

        for each in response.xpath('//div[@class="s-card-inner"]'):
            href = each.xpath('div/div[@class="col-sm-7 s-col-data"]/h2/div/a/@href')
            item = CandidatescraperItem()
            item['sector'] = response.xpath('//div[@id="dropdownMenu1"]/text()').extract_first()
            item['location'] = response.xpath('//div[@id="dropdownMenu2"]/text()').extract_first()
            item['source'] = {"BabaJob"}
            date = time.strftime("%d/%m/%Y")
            # print("date----------------------------------")
            # print(date)
            item['date']=date
            time.sleep(15)
            url = response.urljoin(href.extract_first())
            req = scrapy.Request(url, callback=self.parse_job_details,meta={'item': item})
            yield req


        if(next is not None):

            url = response.urljoin(next.extract_first())
            paginate_req = scrapy.Request(url, callback=self.parse)
            yield paginate_req
        else:
            return





    def parse_job_details(self, response):
        item = response.meta['item']
        item['name']=response.xpath('//div[@id="cp1_nonDeletedUserContent"]/div/div/div/div/div/div/div/div[@class="col-sm-7"]/div/h1/text()').extract_first()
        try:
            onclick = response.xpath('//div[@id="cp1_shortlistButtonContainer"]/div/a/@onclick').extract_first()
            number=onclick[onclick.find('+'):onclick.find(',')]
            item['mobile']=number.replace("+91","")  #11 is the index number of last digit
            print("item-------------------")
            #print item
            #with open("BabaJobs.txt", "a") as text_file:text_file.write(item['mobile']+"\n")
            #yield item
        except:
            return
        finally:
            yield item







