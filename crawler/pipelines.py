import re
import time
import traceback

from crawler.items import JobItem, SectorItem
from scrapy.exceptions import DropItem
from scrapy import signals
from scrapy.exporters import CsvItemExporter
from boto3.session import Session
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import mimetypes
import smtplib
from crawler import settings
from datetime import datetime
import requests
import json
import os


GOOGLE_PLACES_API_KEY = "AIzaSyBu8-KRJdiU0D5xnu6xTnEZ0dg1SAh_OhM"
GOOGLE_TEXT_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
GOOGLE_DETAIL_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/details/json"


class DynamoDBStorePipeline(object):

    def process_item(self, item, spider):
         if spider.name not in ['babajobs','naukri','indeed','shine', 'olx', 'olx_complete', 'zaubacorp', 'sector', 'sector1']:

            dynamodb_session = Session(aws_access_key_id='AKIAJT6AN3A5WZEZ74WA',
                                       aws_secret_access_key='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',
                                       region_name="us-east-1")
            dynamodb = dynamodb_session.resource('dynamodb', region_name='us-east-1')
            table = dynamodb.Table('prod_candidate_leads')
            name = item['name']
            mobile=item['mobile']
            location=item['location']
            sector=item['sector']
            source=item['source']

            if item['mobile'] is not None :
                 try:
                     response = table.get_item(
                          Key={
                              'mobile_number':mobile,
                          }
                     )
                     getitem = response['Item']
                     print(getitem)
                     print("GetItem succeeded:")
                 except :
                     table.put_item(
                         Item={
                             'name': name,
                             'mobile_number': mobile,
                             'location': location,
                             'sector_applied': sector,
                             'source': source,
                             'date': item['date'],
                         }
                     )
                     return item

                 else:

                     src = getitem['source']
                     print(src)
                     for i in source:
                         strs = i
                     if strs not in src :
                         src.add(strs)
                         source=src

                 finally:
                    table.put_item(
                         Item={
                             'name':name,
                             'mobile_number': mobile,
                             'location':location,
                             'sector_applied':sector,
                             'source':source,
                             'date':item['date'],
                         }
                     )
                    return item

         return item


class NewDynamoPipeline(object):
    def process_item(self, item, spider):
        if isinstance(item, SectorItem):
            dynamodb_session = Session(aws_access_key_id='AKIAJT6AN3A5WZEZ74WA',
                                       aws_secret_access_key='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',
                                       region_name="us-east-1")
            dynamodb = dynamodb_session.resource('dynamodb', region_name='us-east-1')
            try:
                try:
                    phone = item['number']
                    if phone is None or len(phone) < 8:
                        return item
                    company_name = 'NA' if item['company_name'] is None else item['company_name']
                    table = dynamodb.Table('phone_no_leads')
                    table.put_item(Item={'phone': phone, 'company': company_name})
                except KeyError:
                    pass

                try:
                    email = item['email']
                    if email is None or len(email) < 4:
                        return item
                    company_name = 'NA' if item['company_name'] is None else item['company_name']
                    table = dynamodb.Table('email_leads')
                    table.put_item(Item={'email': email, 'company': company_name})
                except KeyError:
                    pass
            except:
                print("Suppressed error {}".format(traceback.format_exc()))
            return item


class SectorSpiderCleaning(object):
    def process_item(self, item, spider):
        if spider.name in ['sector', 'sector1']:
            if isinstance(item, SectorItem):
                def __contains(name, keyword): name.lower().find(keyword.lower()) > -1

                try:
                    number = item['number']
                    if number is not None and len(str(number)) > 0:
                        number = list(map(lambda x: x.strip(), re.findall(r'[0-9]{10}|[0-9]{8}', number)))
                        item['number'] = ', '.join(number)

                    contact_name = item['contact_person_name']
                    if contact_name is not None and __contains('call', contact_name):
                        item['contact_name'] = contact_name.split('call')[1].strip()

                    location = item['location']
                    if location is not None and len(location) > 0:
                        l = list(map(lambda x: x.strip() if len(x) > 1 else None, set(location.split(','))))
                        try:
                            l.remove(None)
                            l.remove('more')
                        except ValueError:
                            pass
                        item['location'] = ', '.join(l)
                except:
                    # print("Suppressed error {}".format(traceback.format_exc()))
                    pass
        return item


class SectorSpiderFiltering(object):
    def process_item(self, item, spider):
        if spider.name in ['sector', 'sector1']:
            if isinstance(item, SectorItem):
                def __contains(name, keyword): name.lower().find(keyword.lower()) > -1

                NAMES = ['Aegis Consultant', 'Alaric Human Consulting', 'Allstate Group', 'Alpine Management', 'Anther HR', 'Bharadwaj CareerSolutions', 'Busisol Sourcing India Private Limited', 'Busisol Sourcing India Pvt Ltd', 'Career Craft Consultants', 'Career Creators Group', 'career placements', 'CAREER VISION HR CONSULTANCY', 'CareerGrow pvt.ltd.', 'Careerz Inn', 'CareerzInn Placement Consultancy Pvt Ltd', 'Chandni Consultant', 'CL(Career Launcher) Educate Ltd', 'Clients of company', 'Dimension Placement Services', 'Dynamic manpower services', 'Eka Consultants', 'Eminent placements', 'EQS Placement Pvt Limited', 'ERM Placement Services (P) Ltd.', 'Excel Consultancy', 'Excel Consultancy Services', 'Excel Employment', 'Excellence Recruitment Solution', 'Gips Consultancy Services', 'Global Talent Search', 'Head Hunterz', 'HEADHUNTERS HR PVT LTD', 'Heads2you', 'Hiring Solution', 'Home jobs naukri', 'HOME PASSION', 'HR and PR Solution', 'HRD House', 'HRD india', 'HREsential', 'Human Capital Corporation', 'Hy Fly Consultancy', 'HY FLY CONSULTANCY', 'ijobscity', 'Impeccable HR Consulting Pvt Ltd', 'Inspire HR Services', 'Inteegrity Solutions', 'Intellecta Consultants', 'Invent Careers', 'Job Alerts Services', 'JOB HUNT', 'JOBCITY', 'jobs expert', 'Jobspot HR Services .', 'Jobtrack Management Services Pvt Ltd', 'Joy Recruitment', 'LG Recruitment technology', 'Lobo Staffing Solution Pvt Ltd', 'Make My Dream Job', 'Mangalam Placements Pvt Ltd', 'ManpowerGroup Services', 'Miracle HR', 'Morpheus Human Consulting Pvt Ltd', 'Mumbai Rozgaar Private Limited', 'naukari services pvt ltd', 'naukari services', 'naukari sevices pvt ltd', 'Naukri home based jobs', 'Ontrack HR Services Pvt Limited', 'People Konnect', 'Peopleplease Consulting', 'perfect solution', 'Placewell Consultant', 'Pplanet HR Services', 'Premium-Jobs', 'PRISM MANPOWER SERVICES', 'PYLON Management Consulting Private Limited', 'Quotient Consultancy', 'R max company pvt. ltd', 'R MAX company pvt.Ltd', 'r max', 'R.Max pvt.ltd', 'RAMM Consultancy', 'Recruise', 'RecruiterLane Services', 'satyam manpower solution', 'Seven Consultancy', 'Shine Hiring', 'SHREE SATGURU COLLARS', 'Skill Groomers Management Services Pvt Ltd', 'Skill Groomers Management Services Pvt. Ltd.', 'SKILL VENTORY', 'Skynet Placements', 'SMB HR Solutions', 'Spectrum Talent Management', 'Staffopedia Consulting LLP', 'STEP Placements', 'success maker pvt.ltd.', 'Success Manpower Services1', 'successmanpower', 'SWAN Solutions and Services Pvt Ltd', 'Talent HR Networks Pvt Ltd', 'Talent Hub Jobs', 'Talent Hunters Manpower Solutions', 'Talentmint Consulting Pvt Ltd', 'Talific Consulting Services Pvt. ltd', 'Tanish', 'Tanishka services', 'Tanvi Manpower', 'TeamLease Services Limited', 'TeamLease Services Ltd', 'TR Manpower', 'Transcend Consulting Pte Ltd', 'Transcend HR solutions', 'UNIQUE MANAGEMENT CONSULTANTS', 'Up Man Placements', 'Ushema HR Solutions', 'Ushta Te HR Consultancy LLP', 'Vaishali Hr Solution', 'Value Expert', 'Veeyu HR Solutions Pvt Ltd', 'Wize career Consultants', 'work 24x7', 'Yasron Consultants', 'R Max Pvt Ltd', 'Ram consultancy', 'miss shilpi', 'Skill Groomers Management Services Pvt. Ltd.', 'net star infotech', 'Sudhakar', 'aarav singh', 'Mortage Solutions', 'C. G Ndt services', 'Ashirwad enterprises', 'Global pvt ltd', 'Trixie Impex pvt ltd', 'Tanishka', 'Global pvt ltd', 'Value expert company', 'R Square Infocom Services', 'Swastik Enterprises Mumbai', 'raj sawant', 'Emphasis corporate services', 'Global manpower', 'Shublabh placement', 'Value expert', 'Shublabh placement', 'A.S.Solution', 'Hi-tech', 'Guru kripa placement', 'steno house', 'Saral rozgaar', 'Ashok', 'Swastik enterprises', 'Life info line', 'Akash Kothari', 'santosh patil', 'Shyam', 'Net star infotech', 'Net star infotech', 'Contact Advt.', 'united capital club', 'ucc', 'BDS Services']
                NUMBERS = ['8898013240', '7065582443', '7710009116', '8898001570', '9594536191', '9619689051', '7666607481', '9773755542', '7208458271', '7208458271', '8879403886', '27831123', '9967685174', '9029782234', '89766 84213', '9820023937', '9920287557', '9920593001', '7506375526', '8286200305', '7678035057', '7208348597', '7678035057', '86525 12543', '8268170727', '8898125403', '9871176333', '8446071094', '9871447318', '8692991179', '8291088736', '9833780095', '9594999912', '9730708656', '8424060195', '8898001570', '9029010467', '9867017486', '8424007449', '9920287557', '8108838929', '9930661023', '9702639819', '9920793413', '9220401782', '9022443885', '9820914640', '9920527787', '9769409632', '9833824644', '9833175983', '9987376123', '8879403886', '9820975044', '9819443955', '808080208', '26556095', '9930086572', '8655148633', '7208343050', '9167650003', '8826381078']
                KEYWORDS = ['hr consultant', 'hr services', 'placement', 'human resource', 'staff', 'manpower', 'sourcing', 'employment', 'rozgar', 'rozgaar', 'hiring for']

                try:
                    company_name = item['company_name']
                    if company_name is not None:
                        company_name = company_name.strip()
                        for name in NAMES+KEYWORDS:
                            if __contains(company_name, name):
                                raise DropItem("Dropped item based on filtering {}".format(company_name))
                    number = item['number']
                    if number is not None and len(str(number)) > 0:
                        number = number.strip()
                        for phone in NUMBERS:
                            if __contains(number, phone):
                                raise DropItem("Dropped item based on filtering {}".format(company_name))
                except DropItem:
                    raise DropItem("Dropped item based on filtering {}".format(company_name))
                except Exception as exc:
                    pass
        return item


class FetchGoogleDataPipeline(object):
    def process_item(self, item, spider):
        if spider.name in ['sector', 'sector1']:
            if isinstance(item, SectorItem):
                if item['company_name'] is not None and len(item['company_name']) < 5:
                    return item
                payload = {
                    'query': item['company_name']+' mumbai',
                    'key': GOOGLE_PLACES_API_KEY
                }
                textsearch = requests.get(GOOGLE_TEXT_SEARCH_URL, params=payload)
                if textsearch.status_code == 200 and textsearch.json()['status'] == 'OK':
                    data = textsearch.json()['results'][0]
                    payload = {
                        'placeid': data['place_id'],
                        'key': GOOGLE_PLACES_API_KEY
                    }
                    detailsearch = requests.get(GOOGLE_DETAIL_SEARCH_URL, params=payload)
                    if detailsearch.status_code == 200 and detailsearch.json()['status'] == 'OK':
                        data = detailsearch.json()['result']
                        try:
                            item['google_address'] = data['formatted_address']
                        except KeyError:
                            pass
                        try:
                            item['google_phone_number'] = data['international_phone_number']
                        except KeyError:
                            pass
                        try:
                            for ac in data['address_components']:
                                if 'sublocality_level_1' in ac['types']:
                                    item['station'] = ac['long_name']
                        except KeyError:
                            pass
                    else:
                        print("Detail search failed: " + detailsearch.text.encode('utf-8'))
                else:
                    print("Text search failed: " + textsearch.text.encode('utf-8'))
        return item


class CSVExportPipeline(object):
    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        if spider.name in ['zaubacorp', 'sector', 'sector1']:
            filename = '%s-jobs-%s.csv' % (spider.name, datetime.utcnow().strftime('%d%m%Y%H%M%s'))
            path = os.path.expanduser("/tmp/jobs-data/%s" % filename)
        else:
            filename = '%s_cand_%s.csv' % (spider.name, time.strftime("%d_%m_%Y"))
            path = os.path.expanduser("/tmp/candidate/%s" % filename)
        file = open(path, 'w+b')
        self.files[spider] = file
        self.exporter = CsvItemExporter(file)
        self.exporter.start_exporting()

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        file = self.files.pop(spider)
        filename = file.name
        file.close()
        if spider.name in ['zaubacorp', 'sector', 'sector1']:
            self._send_email(filename)
        else:
            pass
            # self._send_candidate_email(filename)

    def process_item(self, item, spider):
        if spider.name not in ['zaubacorp', 'sector', 'sector1']:
            src = " "
            for i in item['source']:
                src = src + "," + i
            source = src.replace(" ,", "")
            item['source'] = source

        self.exporter.export_item(item)
        return item

    def _send_email(self, filename):
        print('====sending email %s ====' % filename)
        msg = MIMEMultipart('alternative')
        msg['From'] = "contactswa@workindia.in"
        msg['To'] = "nishit.kagalwala@workindia.in"
        msg['Subject'] = 'Portal scraping - %s' % datetime.utcnow().strftime('%d-%b-%Y')
        # attach the csv file
        file = open(filename)
        attachment = MIMEText(file.read(), _subtype='csv')
        file.close()
        attachment.add_header('Content-Disposition', 'attachment', filename=filename.split('/')[-1].strip())
        msg.attach(attachment)

        s = smtplib.SMTP_SSL("smtp.gmail.com", 465)
        s.login("contactswa@workindia.in", "Nishit123")
        # s.sendmail("contactswa@workindia.in", ["sales-workindia@workindia.in",
        #                                        "sahil.gandhi@workindia.in",
        #                                        "moiz.arsiwala@workindia.in"],
        #                                        msg.as_string())
        s.sendmail("admin@workindia.in", ["sahil.gandhi@workindia.in"], msg.as_string())

    def _send_candidate_email(self, filename):
        print('====sending email %s ====' % filename)
        msg = MIMEMultipart('alternative')
        msg['From'] = "contactswa@workindia.in"
        msg['To'] = "nishit.kagalwala@workindia.in"
        msg['Subject'] = 'Candidate scraping - %s' % datetime.utcnow().strftime('%d-%b-%Y')
        # attach the csv file
        file = open(filename)
        attachment = MIMEText(file.read(), _subtype='csv')
        file.close()
        attachment.add_header('Content-Disposition', 'attachment', filename=filename.split('/')[-1].strip())
        msg.attach(attachment)

        s = smtplib.SMTP_SSL("smtp.gmail.com", 465)
        s.login("contactswa@workindia.in", "Nishit123")
        s.sendmail("contactswa@workindia.in", ["ganesh.baleri@workindia.in","abhishek.agarwal@workindia.in","nishit.kagalwala@workindia.in"], msg.as_string())
