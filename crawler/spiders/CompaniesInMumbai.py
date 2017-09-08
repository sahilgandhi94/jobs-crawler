"""
This spider does a full text search for phone nos and email address in all the pages crawled.
Phone regex: \b[789]\d{2}[-.]?\d{3}[-.]?\d{4}\b
Email regex: [\w\.]+@[\w\.]+\.\w+

"""
import re
import requests
import traceback

from boto3.session import Session
from urlparse import urlparse, parse_qs, urljoin

import scrapy


AJAX_URLS = [

    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Accounting/Consulting/%20Taxation&scrpt_name=accounting_taxation_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=IT-%20e-commerce/Internet&scrpt_name=e-commerce_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Airlines/Aviation&scrpt_name=airlines-aviation_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=IT-%20Hardware&scrpt_name=hardware_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Automobiles&scrpt_name=automobiles_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=IT-%20Systems/EDP/MIS&scrpt_name=edp_mis_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Call%20Centres&scrpt_name=call_centres_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Law%20/%20Legal%20Consultants&scrpt_name=law_legalconsultants_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Management%20/Engineering%20/Environ.%20Consultants&scrpt_name=managementconsultants_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Real%20Estate%20Agents&scrpt_name=realestateagents_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Mutual%20Fund/%20Stock%20Broking&scrpt_name=brokingfirms_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Engineering&scrpt_name=engineering_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Paints&scrpt_name=paints_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=FMCG&scrpt_name=fmcg_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Garment%20/%20Textiles/%20Accessories&scrpt_name=garment_textiles_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Power/Energy&scrpt_name=power_energy_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Hospitals/Healthcare&scrpt_name=hospitals_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Security&scrpt_name=security_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Institutes%20-%20Management&scrpt_name=management_institutes_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Sugar&scrpt_name=sugar_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Insurance&scrpt_name=insurance_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Iron%20and%20Steel&scrpt_name=iron-steel_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Agriculture/Dairy/Fertlizer&scrpt_name=agriculture_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=IT-%20ERP/CRM&scrpt_name=erp_crm_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Auto%20Ancillaries/%20Auto%20components&scrpt_name=auto-ancillaries_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=IT-%20QA/Testing&scrpt_name=IT_qa_testing_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=BPO%20/%20KPO&scrpt_name=bpo-kpo_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=IT-Software%20Services&scrpt_name=software_services_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Cement/Marble/%20Ceramics/Stones&scrpt_name=cementmarble-ceramicsstones.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Construction%20/%20Real%20%20Estate&scrpt_name=realestate_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Media%20/%20Entertainment&scrpt_name=media_entertainment_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Electrical/Electronics&scrpt_name=electrical_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Office%20Automation&scrpt_name=officeautomation_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Financial%20Services&scrpt_name=financialservices_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Petrochemicals/%20Oil/%20Gas/%20Refineries&scrpt_name=petrochemicals_companies_mumbai.phphttp://",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Placement%20/%20HR%20/%20Training%20Consultants&scrpt_name=placement_consultants_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Govt/Defence/Embassies&scrpt_name=defence_embassies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Rubber/Plastic/%20Glass/%20Wood&scrpt_name=rubber_plastic_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Institutes%20-%20Engineering&scrpt_name=engineering_institute_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Sports/Toys&scrpt_name=sports_toys_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Institutes%20-%20Schools/Colleges&scrpt_name=school-colleges_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Metals/Mining&scrpt_name=metals_mining_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Advertising/Event%20Mgmt/%20PR/MR&scrpt_name=advertising_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=IT-%20Embedded/EDA/VLSI&scrpt_name=IT-embedded_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Architectural%20Services/%20Interior%20Designing&scrpt_name=architect-interior_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=IT-%20Network%20Admin./Security&scrpt_name=networkadmin_security_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Banks&scrpt_name=banking_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=IT-%20Telecom%20/Mobile&scrpt_name=telecom_mobile_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Capital%20Goods%20/Machine%20Manufacturing&scrpt_name=capital_goods_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Leather/%20shoes/%20Accessories&scrpt_name=leathershoes_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Chemical&scrpt_name=chemicals_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Travel%20/%20Tourism&scrpt_name=travel_tourism_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Courier/%20Logistics/%20Packaging/%20Transportation&scrpt_name=logistics_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=NGOs/World%20Bodies/%20Associations&scrpt_name=ngos_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Export%20Houses&scrpt_name=export-import_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Paper/Publishing/%20Printing/%20Stationary&scrpt_name=paper_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Food%20Processing/%20Beverages&scrpt_name=foodprocessing_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Pharmaceuticals/%20BioTech/%20Research&scrpt_name=pharmaceutical_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Gems%20/Jewelleries/Watches&scrpt_name=gems_jewelleries_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Retail&scrpt_name=retail_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Hotels%20/%20Resorts&scrpt_name=hotels_resorts_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Shipping%20/%20Marine&scrpt_name=shipping_marrine_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Institutes%20-%20Others/%20Universities&scrpt_name=institutes-universities_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Telecommunication/%20Mobile&scrpt_name=telecommunication_companies_mumbai.php",
    "http://www.companiesinmumbai.com/ajax.php?gofor=show_listing&tot_page=80000&page=1&category=Consumer%20Goods%20-%20Durables/Home%20Appliances&scrpt_name=consumergoods_mumbai.php",
]


class LeadsItem(scrapy.Item):
    number = scrapy.Field()
    email = scrapy.Field()
    company_name = scrapy.Field()


class Sector1Spider(scrapy.Spider):
    name = "companiesinmumbai"
    allowed_domains = ["companiesinmumbai.com"]
    start_urls = []

    def __init__(self, **kwargs):
        href_regex = r"href=\"(.*?)\""
        base_url = "http://www.companiesinmumbai.com/"
        for url in AJAX_URLS:
            r = requests.get(url)
            scrapy_urls = re.findall(href_regex, r.text)
            scrapy_urls = list(set(scrapy_urls))
            [self.start_urls.append(urljoin(base_url, _url)) for _url in scrapy_urls]

    def parse(self, response):
        company_name = parse_qs(urlparse(response.url).query).get('id', None)
        number = ','.join(list(set(re.findall(r"\b[789]\d{2}[-.]?\d{3}[-.]?\d{4}\b", str(response.body)))))
        email = ','.join(list(set(re.findall(r"[\w\.]+@[\w\.]+\.\w+", str(response.body)))))

        if company_name is not None:
            company_name = company_name[0].strip()

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
                    table.put_item(Item={'phone': phone, 'company': company_name, 'source': 'companiesinmumbai'})
            except KeyError:
                print("Suppressed error {}".format(traceback.format_exc()))

            try:
                if not (email is None or len(email) < 4):
                    print("Uploading email {}".format(email))
                    company_name = 'NA' if company_name is None else company_name
                    table = dynamodb.Table('email_leads')
                    table.put_item(Item={'email': email, 'company': company_name, 'source': 'companiesinmumbai'})
            except KeyError:
                print("Suppressed error {}".format(traceback.format_exc()))
        except:
            print("Suppressed error {}".format(traceback.format_exc()))

        data = {
            'company_name': company_name,
            'number': number,
            'email': email
        }

