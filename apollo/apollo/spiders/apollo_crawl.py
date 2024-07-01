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
