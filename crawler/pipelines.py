from crawler.items import JobItem
from scrapy.exceptions import DropItem
from scrapy import signals
from scrapy.exporters import CsvItemExporter

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import mimetypes
import smtplib

from datetime import datetime
import requests
import json
import os


GOOGLE_PLACES_API_KEY="AIzaSyBu8-KRJdiU0D5xnu6xTnEZ0dg1SAh_OhM"
GOOGLE_TEXT_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
GOOGLE_DETAIL_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/details/json"

class JobPostProcessingPipeline(object):
    def process_item(self, item, spider):
        if isinstance(item, JobItem):
            print('==== post processing ====')
            _pass = True
            # remove any item that has 'bpo' in it
            if self._contains(item['industry'], 'bpo'):
                raise DropItem("Dropping item because bpo: %s" % item['industry'])
                _pass = False

            # remove if experience_requirements is > 2 years
            # ex str: '2 - 5 yrs', '4 - 7 yrs'
            _temp = item['experience_requirements']
            try:
                if int(_temp[0:_temp.find('-')].strip()) > 1:
                    raise DropItem("Dropping item because exp req > 1 :"  + item['experience_requirements'])
                    _pass = False
            except DropItem as e:
                raise e
            except ValueError:
                pass

            if self._contains(item['location'], 'bangalore') or self._contains(item['location'], 'delhi') or self._contains(item['location'], 'kolkata'):
                raise DropItem("Dropping item because location :" + item['location']) 
                _pass = False

            if self._contains(item['company_name'], 'manpower') or self._contains(item['company_name'], 'united capital club') or self._contains(item['company_name'], 'upc') or self._contains(item['company_name'], 'hiring for us based mnc') or self._contains(item['company_name'], 'consultants'):
                raise DropItem("Dropping item because comp name :" + item['company_name']) 
                _pass = False

            if _pass: return item

    def _contains(self, str, key):
        return str.lower().find(key) > -1

class FetchGoogleDataPipeline(object):
    def process_item(self, item, spider):
        # fetch company data from google
        # text search - https://maps.googleapis.com/maps/api/place/textsearch/json?query=peppermint%20communications%20mumbai&key=
        # detail search - https://maps.googleapis.com/maps/api/place/details/json?placeid=ChIJDdjQVhbJ5zsRxM8KlfZaifk&key=
        if isinstance(item, JobItem):
            print('==== fetch data from google ====')
            payload = {
                'query': item['company_name']+' mumbai',
                'key': GOOGLE_PLACES_API_KEY
            }

            textsearch = requests.get(GOOGLE_TEXT_SEARCH_URL, params=payload)
            if textsearch.status_code == 200 and textsearch.json()['status'] == 'OK':
                data = textsearch.json()['results'][0]
                item['place_id'] = data['place_id']

                payload = {
                    'placeid': item['place_id'],
                    'key':GOOGLE_PLACES_API_KEY
                }
                detailsearch = requests.get(GOOGLE_DETAIL_SEARCH_URL, params=payload)
                if detailsearch.status_code == 200 and detailsearch.json()['status'] == 'OK':
                    print('=== getting google detail data ===')
                    data = detailsearch.json()['result']
                    try:
                        item['google_url'] = data['url']
                    except KeyError:
                        pass
                    try:
                        item['google_address'] = data['formatted_address']
                    except KeyError:
                        pass
                    try:
                        item['international_phone_number'] = data['international_phone_number']
                    except KeyError:
                        pass
                    try:
                        item['formatted_phone_number'] = data['formatted_phone_number']
                    except KeyError:
                        pass
                    try:
                        item['website'] = str(item['website']) + ", " + str(data['website'])
                    except KeyError:
                        pass
                    try:
                        item['latitude'] = data['geometry']['location']['lat']
                    except KeyError:
                        pass
                    try:
                        item['longitude'] = data['geometry']['location']['lng']
                    except KeyError:
                        pass
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
        filename = '%s-jobs-%s.csv' % (spider.name, datetime.utcnow().strftime('%d%m%Y%H%M%s'))
        path = os.path.expanduser("/tmp/jobs-data/%s" % filename)
        file = open(path, 'w+b')
        self.files[spider] = file
        self.exporter = CsvItemExporter(file)
        self.exporter.start_exporting()

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        file = self.files.pop(spider)
        filename = file.name
        file.close()
        self._send_email(filename)

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def _send_email(self, filename):
        print('====sending email %s ====' % filename)
        msg = MIMEMultipart('alternative')
        msg['From'] = "admin@workindia.in"
        msg['To'] = "sales-workindia@workindia.in"
        msg['Subject'] = 'Portal scraping - %s' % datetime.utcnow().strftime('%d-%b-%Y')
        # attach the csv file
        file = open(filename)
        attachment = MIMEText(file.read(), _subtype='csv')
        file.close()
        attachment.add_header('Content-Disposition', 'attachment', filename=filename.split('/')[-1].strip())
        msg.attach(attachment)

        s = smtplib.SMTP_SSL("smtp.gmail.com", 465)
        s.login("admin@workindia.in", "28092263")
        s.sendmail("admin@workindia.in", ["sales-workindia@workindia.in, sahil.gandhi@workindia.in, moiz.arsiwala@workindia.in"], msg.as_string())
