# -*- coding: utf-8 -*-

from crawler.items import JobItem
from scrapy.exceptions import DropItem


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
                if int(_temp[0:_temp.find('-')].strip()) > 2:
                    raise DropItem("Dropping item because exp req > 2 :"  + item['experience_requirements'])
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
