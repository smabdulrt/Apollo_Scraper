import csv
import json
import logging
import os
import random
import string
import time
from datetime import datetime

from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
import pandas as pd
from scrapy import Spider, Request, FormRequest

# from .helpers import fill_people_df_1
EMAIL = "" #TODO ENter your email
PASSWORD = "" # TODO Enter your password


class ApolloCrawlSpider(Spider):
    name = "apollo_crawl"
    companies_columns = [
        "id", "Company", "Company LinkedIn", "Company Website", "Company Apollo URL",
        "Annual Revenue", "Industry", "Employees", "Founded Year", "Funding Round",
        "Company Description", "Company Keywords"
    ]

    def __init__(self):
        with open("List_of_ZIP_Code_prefixes_3.csv", "r") as file:
            zip_codes = list(csv.DictReader(file))
        self.priority_zip_codes = {"A": [],
                                   "B": [],
                                   "C": [],
                                   "N": []}
        for zip_code in zip_codes:
            try:
                self.priority_zip_codes[zip_code.get("Priority ", '')].append(zip_code)
            except:
                data = ''
        pass

    def start_requests(self):
        cahcekey = "{0}{1}{2}{3}{4}".format(*[
            random.randint(5, 9),
            random.randint(0, 9),
            random.randint(0, 9),
            random.randint(0, 9),
            random.randint(0, 9)
        ])

        m_headers = {
            "authority": "app.apollo.io",
            "content-type": "application/json",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }
        params = {
            "timezone_offset": "-180",
            "current_finder_view_id": "",
            "cacheKey": "16917008{0}".format(cahcekey)
        }
        yield FormRequest(url='https://app.apollo.io/api/v1/auth/check', formdata=params, headers=m_headers,
                          callback=self.parse_check, method="GET")

    def parse_check(self, response):
        data = response.json()
        token = ''
        for cookie in response.headers.getlist('Set-Cookie'):
            if 'X-CSRF-TOKEN' in cookie.decode("utf-8"):
                token = cookie.decode('utf-8').split("; ")[0].split("=")[-1]
        cahcekey = "{0}{1}{2}{3}{4}".format(*[
            random.randint(5, 9),
            random.randint(0, 9),
            random.randint(0, 9),
            random.randint(0, 9),
            random.randint(0, 9)
        ])
        headers = {
            "authority": "app.apollo.io",
            "content-type": "application/json",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "x-csrf-token": token
        }
        json_data = {
            "email": EMAIL,
            "password": PASSWORD,
            "timezone_offset": "-180",
            "cacheKey": "16917009{0}".format(cahcekey)
        }
        yield Request("https://app.apollo.io/api/v1/auth/login", headers=headers, body=json.dumps(json_data),
                      method="POST",
                      callback=self.parse_login)

    def parse_login(self, response):
        data = response.json()
        token = ''
        for cookie in response.headers.getlist('Set-Cookie'):
            if 'X-CSRF-TOKEN' in cookie.decode("utf-8"):
                token = cookie.decode('utf-8').split("; ")[0].split("=")[-1]
        for key, zip_codes in self.priority_zip_codes.items():
            if "N" in key:
                continue
            for zip_code in zip_codes:
                for iterate in range(1, 100):
                    seed = "".join(random.choices(string.ascii_lowercase + string.digits, k=random.randint(10, 11)))
                    page = 1
                    # pages = 5
                    # while page < pages:
                    cahcekey = "{0}{1}{2}{3}{4}{5}{6}".format(*[
                        random.randint(0, 9),
                        random.randint(0, 9),
                        random.randint(0, 9),
                        random.randint(0, 9),
                        random.randint(0, 9),
                        random.randint(0, 9),
                        random.randint(0, 9)
                    ])

                    headers = {
                        "authority": "app.apollo.io",
                        "content-type": "application/json",
                        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
                        "x-csrf-token": token,
                    }
                    json_data = {
                        "finder_table_layout_id": None,
                        # "person_titles": [
                        #     "ceo",
                        #     "chief executive officer",
                        # ],
                        "person_titles": [
                            "cfo",
                            "chief financial officer",
                            "vp finance",
                        ],
                        "page": page,
                        "person_location_name": str(f"{zip_code.get('Prefix', '')}{iterate:02}").strip(" '"),
                        "person_location_radius": "25",
                        "display_mode": "explorer_mode",
                        "per_page": 25,
                        "open_factor_names": [],
                        "num_fetch_result": 1 + page,
                        "context": "people-index-page",
                        "show_suggestions": False,
                        "ui_finder_random_seed": seed,
                        "cacheKey": int("170197{0}".format(cahcekey)),
                    }
                    yield Request(url='https://app.apollo.io/api/v1/mixed_people/search',
                                  headers=headers,
                                  body=json.dumps(json_data),
                                  method='POST',
                                  callback=self.parse_people, meta={'page': page, "token": token, "zip_code": str(
                            f"{zip_code.get('Prefix', '')}{iterate:02}").strip(" '")})
                # page += 1

    def parse_people(self, response):
        data = response.json()
        results = data["people"]
        page = response.meta['page']
        # cahcekey = response.meta['cahcekey']
        zip_code = response.meta['zip_code']
        token = response.meta['token']
        check = ''
        for result in results[:]:
            details = dict()
            details['First Name'] = result.get('first_name', '')
            details['Last Name'] = result.get("last_name", '')
            details['LinkedIn'] = result.get("linkedin_url", '')
            details['Apollo URL'] = "https://app.apollo.io/#/people/{0}".format(result.get("id", ''))
            details['Title'] = result.get("title", '')
            if "United States" in result.get("country", ''):
                details['Contact Location'] = "{0}, {1}, {2}".format(result.get("city", ''), result.get("state", ''),
                                                                     result.get("country", ''))
                index = 1
                for employment in result["employment_history"]:
                    org_id = employment.get("organization_id")
                    if not org_id:
                        continue
                    m_headers = {
                        'accept': '*/*',
                        'accept-language': 'en-US,en;q=0.9',
                        'content-type': 'application/json',
                        # 'cookie': '__stripe_mid=fb78734e-426d-4505-b12b-6fdeceec8f0869ca99; mutiny.user.token=1e587eee-7d1d-4094-af50-886fa2733225; zp__initial_referrer=https://www.google.com/; zp__utm_source=www.google.com; zp__initial_utm_source=www.google.com; hubspotutk=dba01b95601d50059a8a3c2f73f27f3e; _gcl_au=1.1.583892813.1708598560; _ga=GA1.1.598069358.1708598559; _fbp=fb.1.1708598560186.1746422488; intercom-device-id-dyws6i9m=2550e501-59d7-4960-918d-df162992899a; _cioanonid=bc54776d-d22f-6137-faff-96ec1f24f48c; zp__utm_medium=(none); zp__initial_utm_medium=(none); zp__utm_campaign=pricing_page; zp__initial_utm_campaign=pricing_page; zp__utm_content=buy_now; zp__initial_utm_content=buy_now; ps_mode=trackingV1; _hjSessionUser_3601622=eyJpZCI6IjE4YjY3NTIzLWM1MTAtNWE2My1hODIwLTE3Y2UwMzU0MjlmMiIsImNyZWF0ZWQiOjE3MTA1MTMzMTE4NzMsImV4aXN0aW5nIjp0cnVlfQ==; remember_token_leadgenie_v2=eyJfcmFpbHMiOnsibWVzc2FnZSI6IklqWTFaamcyTWpFMU9UWTNZakUxTURGak5tVmtPR1k0TkY4ME5UVmhNR014T1RrNVpXRTROekE0Wm1Vek1qTmlabUpoWkRObVpEa3pPU0k9IiwiZXhwIjoiMjAyNC0wNS0xNlQwOTo0Njo0OS4zNDlaIiwicHVyIjoiY29va2llLnJlbWVtYmVyX3Rva2VuX2xlYWRnZW5pZV92MiJ9fQ%3D%3D--19b6fbfe41b0564b2bc3b2a1dbc3aa94b6096d33; _cioid=65f86215967b1501c6ed8f84; __hssrc=1; _clck=xwn3tp%7C2%7Cfl2%7C0%7C1535; ZP_LATEST_LOGIN_PRICING_VARIANT=23Q4_EC_Z59; ZP_Pricing_Split_Test_Variant=23Q4_EC_Z59; __q_state_xnwV464CUjypYUw2=eyJ1dWlkIjoiNjg3ZmNjN2MtMzZkZS00ODI0LThjMTUtMzkzODljN2MxY2Y5IiwiY29va2llRG9tYWluIjoiYXBvbGxvLmlvIiwibWVzc2VuZ2VyRXhwYW5kZWQiOmZhbHNlLCJwcm9tcHREaXNtaXNzZWQiOmZhbHNlLCJjb252ZXJzYXRpb25JZCI6IjEzNzgzNTkyMzk3MzgxMTUyNzIifQ==; _ga_76XXTC73SP=GS1.1.1713520738.5.0.1713520751.47.0.2117611735; mutiny.user.session=d06596e0-1653-4984-9533-80cc28754d71; mutiny.user.session_number=5; __hstc=21978340.dba01b95601d50059a8a3c2f73f27f3e.1708598559408.1713520737513.1713525560372.9; __stripe_sid=b320583c-54bc-4a28-934d-250a539c2c1967d2b3; GCLB=CNO4q7rh9f61SRAD; __hssc=21978340.3.1713525560372; _clsk=7rtx5a%7C1713526670746%7C3%7C1%7Cn.clarity.ms%2Fcollect; intercom-session-dyws6i9m=Wi9PK3BLY1lyN2xicEZiSkhQZnhLQ3RSa25IdlR0SlBrZ2lGNWpmNC9DR3J6b1FTbitjQXVaK3JlM3ZORERmcy0tSlYrSWRudzlFSHZnaHlkeVNRY09JUT09--519cab525b4847b12af543086398a32f4ae7dc9a; amplitude_id_122a93c7d9753d2fe678deffe8fac4cfapollo.io=eyJkZXZpY2VJZCI6IjdkMzkyZDk2LTM5ZDQtNDEwMy04NWQyLTk1NmY5YmFiZjI0ZFIiLCJ1c2VySWQiOiI2NWY4NjIxNTk2N2IxNTAxYzZlZDhmODQiLCJvcHRPdXQiOmZhbHNlLCJzZXNzaW9uSWQiOjE3MTM1MjU1NTMwOTMsImxhc3RFdmVudFRpbWUiOjE3MTM1MjY2NzU2NzMsImV2ZW50SWQiOjM1LCJpZGVudGlmeUlkIjo1Miwic2VxdWVuY2VOdW1iZXIiOjg3fQ==; X-CSRF-TOKEN=DdWaSLWrjBuV-GQrhI3u5QRxXF-EAGGj6eZwr2nqvfSpMCvKl8xJilGynBjml4YUlcrxoXn_PTVKwX9ixdE9Yg; _leadgenie_session=ROHEX7RGUypMxB1pqUTmLvN4i1oMpkuRXnC0uFGacL9ywZ2j7TAnvlmL%2F03VywvUnyMFDzA2uPiPhSvNsiLkQmtGjTVjK3cIrPIJvFFz0pFEwjv8RzpiqvANdb%2By4Jie03EKtAZoVt%2FEX30O2QHoNazNio8aw7nEfaatUHVRkkanKq%2B1r%2BDNMe37zWxJJiwz3zA8yBo3Lov98iw%2Bqyhz0SVXZZAIqUtOCambjgLyiEEejwxp6W5Sy7XUz%2FYI5W%2B8nb2xJGkfewQHTl74BmDrgflX5oepWikrP%2B8%3D--y7LRgd2P%2FDsnqT4N--1F9oi46eWaM5wxRDhyxrdA%3D%3D; _dd_s=rum=0&expire=1713527565444&lock=fd531072-fc90-408a-af3f-36741664965b; X-CSRF-TOKEN=9Pe_Lm3qkBwQDzfL91iPT2cEWiMa-4ENdfDz25qHcupQEg6sT41VjdRFz_iVQue-9r_33ecE3ZvW1_wWNrzyfA; _leadgenie_session=WNj91YWAZ19i9FVMkKgUMZZ97cGgGofFbINEp3tirMZzZZXAlNCWetVRZeCSFktWd3vLN4XzG5yyjuvl1%2FsVvhHQex9j%2F0K%2B6HAK9Kq38Bxt4VylfLJwNuqJdUGFWTRW0zk%2Fq1ltjIj5UqWN%2BlU6b99P7xHOT4b7Nd8BS05QiALFQcw31VoJ4lDM%2BWWi1PODmTY9oqPTGEJJyIWfnX%2FqdX3neHRR0kbeTN7FmbtUn71sWqobxJM32xbOkuOuvNQLwvNotyTlSZwkjuWXFzAoyAXGuF95VNam8nA%3D--lMRPnc3tfWZQ5voz--XVpWt8C5Nr%2BGVyV%2FQV6IzA%3D%3D',
                        'referer': 'https://app.apollo.io/',
                        'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"Windows"',
                        'sec-fetch-dest': 'empty',
                        'sec-fetch-mode': 'cors',
                        'sec-fetch-site': 'same-origin',
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
                    }
                    current_timestamp = int(time.time() * 1000)
                    yield Request(
                        url=f'https://app.apollo.io/api/v1/organizations/{org_id}?cacheKey={current_timestamp}',
                        headers=m_headers, meta={"details": details,
                                                 "index": index,
                                                 "employment": employment},
                        callback=self.parse_company)
                    index += 1
        if results:
            page += 1
            seed = "".join(random.choices(string.ascii_lowercase + string.digits, k=random.randint(10, 11)))

            cahcekey = "{0}{1}{2}{3}{4}{5}{6}".format(*[
                random.randint(0, 9),
                random.randint(0, 9),
                random.randint(0, 9),
                random.randint(0, 9),
                random.randint(0, 9),
                random.randint(0, 9),
                random.randint(0, 9)
            ])

            headers = {
                "authority": "app.apollo.io",
                "content-type": "application/json",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
                "x-csrf-token": token,
            }
            json_data = {
                "finder_table_layout_id": None,
                # "person_titles": [
                #     "ceo",
                #     "chief executive officer",
                # ],
                "person_titles": [
                    "cfo",
                    "chief financial officer",
                    "vp finance",
                ],
                "page": page,
                "person_location_name": zip_code,
                "person_location_radius": "25",
                "display_mode": "explorer_mode",
                "per_page": 25,
                "open_factor_names": [],
                "num_fetch_result": 1 + page,
                "context": "people-index-page",
                "show_suggestions": False,
                "ui_finder_random_seed": seed,
                "cacheKey": int("170197{0}".format(cahcekey)),
            }
            yield Request(url='https://app.apollo.io/api/v1/mixed_people/search',
                          headers=headers,
                          body=json.dumps(json_data),
                          method='POST',
                          callback=self.parse_people, meta={'page': page, "token": token, "zip_code": zip_code})

    def parse_company(self, response):
        data = response.json()
        details = response.meta['details']
        index = response.meta['index']
        result = data.get('organization')
        employment = response.meta['employment']

        try:
            if employment["current"]:
                date = "{0}:current".format(employment["start_date"])
            else:
                date = "{0}:{1}".format(employment["start_date"], employment["end_date"])
            if employment["start_date"]:
                details["Dates of Employment {0}".format(index)] = date
            else:
                details["Dates of Employment {0}".format(index)] = ''
        except Exception:
            details["Dates of Employment {0}".format(index)] = ''
        details[f'Company {index}'] = result.get('name', '')
        details[f'Company Location {index}'] = result.get("raw_address", '')
        details[f'Company LinkedIn {index}'] = result['linkedin_url']
        details[f'Company Website {index}'] = result['website_url']
        details[f'Company Apollo URL {index}'] = 'https://app.apollo.io/#/organizations/{0}'.format(result['id'])
        details[f'Annual Revenue {index}'] = result.get('annual_revenue', '')
        details[f'Industry {index}'] = result['industry']
        details[f'Employees {index}'] = result['estimated_num_employees']
        details[f'Founded Year {index}'] = result['founded_year']
        if result.get('latest_funding_round_date'):
            details[f'Funding Round {index}'] = str(
                {result['latest_funding_stage']: result['latest_funding_round_date'].split('T')[0]})
        else:
            details[f'Funding Round {index}'] = ''
        details[f'Company Description {index}'] = '{0} {1}'.format(result['seo_description'],
                                                                   result['short_description'])
        details[f'Company Keywords {index}'] = ", ".join(result['keywords']) if result['keywords'] else ''
        yield details
