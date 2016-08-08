import scrapy


class OLX(scrapy.Spider):
    name = "zaubacorp"
    allowed_domains = ["zaubacorp.com"]
    start_urls = []

    def __init__(self, cin=None, **kwargs):
        cins = cin.split(',')
        cins = [str(c).strip() for c in cins]
        company_url = "https://www.zaubacorp.com/company/companyname/__CIN__"
        [self.start_urls.append(company_url.replace('__CIN__', cin)) for cin in cins]

    def parse(self, response):
        rx = response.xpath
        cin = rx(".//*[@id='block-system-main']/div[2]/div[1]/div[1]/div[1]/table/thead/tr/td[2]/p/a/text()").extract_first()
        company_name = rx(".//*[@id='block-system-main']/div[2]/div[1]/div[1]/div[1]/table/tbody/tr[1]/td[2]/p/text()").extract_first()
        roc = rx(".//*[@id='block-system-main']/div[2]/div[1]/div[1]/div[1]/table/tbody/tr[3]/td[2]/p/text()").extract_first()
        registration_number = rx(".//*[@id='block-system-main']/div[2]/div[1]/div[1]/div[1]/table/tbody/tr[4]/td[2]/p/text()").extract_first()
        date_of_incorporation = rx(".//*[@id='block-system-main']/div[2]/div[1]/div[1]/div[1]/table/tbody/tr[8]/td[2]/p/text()").extract_first()
        email = rx(".//*[@id='block-system-main']/div[2]/div[1]/div[6]/div/div[1]/p[1]/text()").extract_first()
        website = rx(".//*[@id='block-system-main']/div[2]/div[1]/div[6]/div/div[1]/p[2]/span/text()").extract_first()
        address = rx(".//*[@id='block-system-main']/div[2]/div[1]/div[6]/div/div[1]/p[4]/text()").extract_first()
        directors = []
        for i in range(0, 10):
            try:
                d = rx(".//*[@id='package{}']/td[2]/p/a/text()".format(i)).extract_first().encode('utf-8')
            except AttributeError:
                continue
            directors.append(d.strip()) if d is not None else ''
        try:
            directors = directors.remove('Click here')
        except ValueError:
            pass
        directors = self._join(directors, ', ')

        data = {
            'CIN': self._encode(cin),
            'Company Name': self._encode(company_name),
            'RoC': self._encode(roc),
            'Registration number': self._encode(registration_number),
            'Date of incorporation': self._encode(date_of_incorporation),
            'Email': self._encode(email),
            'Website': self._encode(website.strip()),
            'Address': self._encode(address),
            'Directors': self._encode(directors)
        }
        print(data)
        return data

    def _encode(self, ob=None):
        return ob.encode('utf-8') if ob is not None else ''

    def _rstrip(self, l):
        return [x.strip().replace("\r\n,", "") for x in l]

    def _join(self, l, delimeter=' '):
        return delimeter.join(self._rstrip(l))  # to remove \r\n characters

