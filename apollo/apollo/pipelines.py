# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import time

import pandas as pd
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class ApolloPipeline:
    column_names = [
        "First Name", "Last Name", "LinkedIn", "Apollo URL", "Title", "Contact Location",
        "Company 1", "Company LinkedIn 1", "Company Website 1", "Company Apollo URL 1", "Dates of Employment 1",
        "Annual Revenue 1", "Industry 1", "Employees 1", "Founded Year 1", "Funding Round 1",
        "Company Description 1", "Company Keywords 1",
        "Company 2", "Company LinkedIn 2", "Company Website 2", "Company Apollo URL 2", "Dates of Employment 2",
        "Annual Revenue 2", "Industry 2", "Employees 2", "Founded Year 2", "Funding Round 2",
        "Company Description 2", "Company Keywords 2",
        "Company 3", "Company LinkedIn 3", "Company Website 3", "Company Apollo URL 3", "Dates of Employment 3",
        "Annual Revenue 3", "Industry 3", "Employees 3", "Founded Year 3", "Funding Round 3",
        "Company Description 3", "Company Keywords 3",
        "Company 4", "Company LinkedIn 4", "Company Website 4", "Company Apollo URL 4", "Dates of Employment 4",
        "Annual Revenue 4", "Industry 4", "Employees 4", "Founded Year 4", "Funding Round 4",
        "Company Description 4", "Company Keywords 4",
        "Company 5", "Company LinkedIn 5", "Company Website 5", "Company Apollo URL 5", "Dates of Employment 5",
        "Annual Revenue 5", "Industry 5", "Employees 5", "Founded Year 5", "Funding Round 5",
        "Company Description 5", "Company Keywords 5",
        "Company 6", "Company LinkedIn 6", "Company Website 6", "Company Apollo URL 6", "Dates of Employment 6",
        "Annual Revenue 6", "Industry 6", "Employees 6", "Founded Year 6", "Funding Round 6",
        "Company Description 6", "Company Keywords 6",
        "Company 7", "Company LinkedIn 7", "Company Website 7", "Company Apollo URL 7", "Dates of Employment 7",
        "Annual Revenue 7", "Industry 7", "Employees 7", "Founded Year 7", "Funding Round 7",
        "Company Description 7", "Company Keywords 7",
        "Company 8", "Company LinkedIn 8", "Company Website 8", "Company Apollo URL 8", "Dates of Employment 8",
        "Annual Revenue 8", "Industry 8", "Employees 8", "Founded Year 8", "Funding Round 8",
        "Company Description 8", "Company Keywords 8",
        "Company 9", "Company LinkedIn 9", "Company Website 9", "Company Apollo URL 9", "Dates of Employment 9",
        "Annual Revenue 9", "Industry 9", "Employees 9", "Founded Year 9", "Funding Round 9",
        "Company Description 9", "Company Keywords 9",
        "Company 10", "Company LinkedIn 10", "Company Website 10", "Company Apollo URL 10", "Dates of Employment 10",
        "Annual Revenue 10", "Industry 10", "Employees 10", "Founded Year 10", "Funding Round 10",
        "Company Description 10", "Company Keywords 10",
        "Company 11", "Company LinkedIn 11", "Company Website 11", "Company Apollo URL 11", "Dates of Employment 11",
        "Annual Revenue 11", "Industry 11", "Employees 11", "Founded Year 11", "Funding Round 11",
        "Company Description 11", "Company Keywords 11",
        "Company 12", "Company LinkedIn 12", "Company Website 12", "Company Apollo URL 12", "Dates of Employment 12",
        "Annual Revenue 12", "Industry 12", "Employees 12", "Founded Year 12", "Funding Round 12",
        "Company Description 12", "Company Keywords 12",
        "Company 13", "Company LinkedIn 13", "Company Website 13", "Company Apollo URL 13", "Dates of Employment 13",
        "Annual Revenue 13", "Industry 13", "Employees 13", "Founded Year 13", "Funding Round 13",
        "Company Description 13", "Company Keywords 13",
        "Company 14", "Company LinkedIn 14", "Company Website 14", "Company Apollo URL 14", "Dates of Employment 14",
        "Annual Revenue 14", "Industry 14", "Employees 14", "Founded Year 14", "Funding Round 14",
        "Company Description 14", "Company Keywords 14",
        "Company 15", "Company LinkedIn 15", "Company Website 15", "Company Apollo URL 15", "Dates of Employment 15",
        "Annual Revenue 15", "Industry 15", "Employees 15", "Founded Year 15", "Funding Round 15",
        "Company Description 15", "Company Keywords 15",
        "Company 16", "Company LinkedIn 16", "Company Website 16", "Company Apollo URL 16", "Dates of Employment 16",
        "Annual Revenue 16", "Industry 16", "Employees 16", "Founded Year 16", "Funding Round 16",
        "Company Description 16", "Company Keywords 16",
        "Company 17", "Company LinkedIn 17", "Company Website 17", "Company Apollo URL 17", "Dates of Employment 17",
        "Annual Revenue 17", "Industry 17", "Employees 17", "Founded Year 17", "Funding Round 17",
        "Company Description 17", "Company Keywords 17",
        "Company 18", "Company LinkedIn 18", "Company Website 18", "Company Apollo URL 18", "Dates of Employment 18",
        "Annual Revenue 18", "Industry 18", "Employees 18", "Founded Year 18", "Funding Round 18",
        "Company Description 18", "Company Keywords 18",
        "Company 19", "Company LinkedIn 19", "Company Website 19", "Company Apollo URL 19", "Dates of Employment 19",
        "Annual Revenue 19", "Industry 19", "Employees 19", "Founded Year 19", "Funding Round 19",
        "Company Description 19", "Company Keywords 19",
        "Company 20", "Company LinkedIn 20", "Company Website 20", "Company Apollo URL 20", "Dates of Employment 20",
        "Annual Revenue 20", "Industry 20", "Employees 20", "Founded Year 20", "Funding Round 20",
        "Company Description 20", "Company Keywords 20",
        "Company 21", "Company LinkedIn 21", "Company Website 21", "Company Apollo URL 21", "Dates of Employment 21",
        "Annual Revenue 21", "Industry 21", "Employees 21", "Founded Year 21", "Funding Round 21",
        "Company Description 21", "Company Keywords 21",
        "Company 22", "Company LinkedIn 22", "Company Website 22", "Company Apollo URL 22", "Dates of Employment 22",
        "Annual Revenue 22", "Industry 22", "Employees 22", "Founded Year 22", "Funding Round 22",
        "Company Description 22", "Company Keywords 22",
        "Company 23", "Company LinkedIn 23", "Company Website 23", "Company Apollo URL 23", "Dates of Employment 23",
        "Annual Revenue 23", "Industry 23", "Employees 23", "Founded Year 23", "Funding Round 23",
        "Company Description 23", "Company Keywords 23",
    ]
    people_data = {}
    batch_number = 1
    details = {key: '' for key in column_names}

    def process_item(self, item, spider):
        details = dict(item)
        if self.people_data.get(details['Apollo URL']):
            self.people_data[details['Apollo URL']].append(details)
        if not self.people_data.get(details['Apollo URL']):
            self.people_data[details['Apollo URL']] = []
            self.people_data[details['Apollo URL']].append(details)
        # if len(self.people_data) == 1000:
        # self.all_persons = []
        all_keys = list(self.people_data.keys())
        if len(all_keys) % 1000 == 0:
            all_persons = []
            for key, records in self.people_data.items():
                person_details = dict()
                all_details = dict()
                for record in records:
                    person_details.update(record)
                for column, value in self.details.items():
                    all_details[column] = person_details.get(column, '')
                all_persons.append(all_details)
            if all_persons:
                df = pd.DataFrame(all_persons)
                df.to_excel(f"Batch_new_{self.batch_number}_Apollo_CFO_PULL_{time.strftime('%d%m%Y %H%M%S')}.xlsx",
                            index=False)
                self.batch_number += 1
        return item

    def close_spider(self, spider):
        all_persons = []
        for key, records in self.people_data.items():
            person_details = dict()
            all_details = dict()
            for record in records:
                person_details.update(record)
            for column, value in self.details.items():
                all_details[column] = person_details.get(column, '')
            all_persons.append(all_details)
        if all_persons:
            df = pd.DataFrame(all_persons)
            df.to_excel(f"Batch_ALL_Apollo_CFO_PULL_{time.strftime('%d%m%Y %H%M%S')}.xlsx", index=False)
