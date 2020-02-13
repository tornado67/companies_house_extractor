#!/usr/bin/env python
from __future__ import print_function

import os
import csv
import sys
import json
import datetime
import argparse
from time import sleep
from requests import HTTPError

from companies_house.api import CompaniesHouseAPI

_NUM_SC_PREF = "SC"
_LAST_FILE_DEFAULT = 'last.json.sample.sample'
_RESULT_CSV_DEFAULT = "result.csv"
_API_KEY = os.getenv('API_KEY')


# ------------------------------------------------------------------------------

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--last", action='store', dest='last_file',
                        help="last file path", default=_LAST_FILE_DEFAULT)
    parser.add_argument("-o", "--out", action='store', dest='result_file',
                        help="Where result will be stored", default=_RESULT_CSV_DEFAULT)
    parser.add_argument("-r", "--ratelimit-freeze", action='store', dest='ratelimit',
                        help="Where result will be stored", default=50)
    parser.add_argument("-e", "--empty-limit", action='store', dest='empty_limit',
                        help="How much empty companies threat as end of list", default=20)

    return parser.parse_args()


# ------------------------------------------------------------------------------

def get_director(number: str, ch: CompaniesHouseAPI) -> str:
    director: str = ""
    psc = ch.list_company_officers(company_number=number)
    if not psc:
        psc = ch.list_company_persons_with_significant_control(company_number=number)
        if not psc:
            psc = ch.list_company_persons_with_significant_control_statements(company_number=number)
            if not psc:
                return None

    if psc.get("active_count") == 1:
        officers = psc.get("items")
        for officer in officers:
            if officer.get("officer_role") == "director":
                director = officer.get("name")
    return director


# ------------------------------------------------------------------------------

# noinspection PyPackageRequirements,PyPackageRequirements
def get_address(company: dict) -> tuple:
    registered_office_address = company.get("registered_office_address")
    address = str(registered_office_address.get("address_line_1"))
    country = str(registered_office_address.get("country"))
    city = str(registered_office_address.get("locality"))
    postal_code = str(registered_office_address.get("postal_code"))

    return address, country, city, postal_code


# ------------------------------------------------------------------------------

def get_company_details(number: str, ch: CompaniesHouseAPI) -> list:
    company: dict = {}
    res = None
    try:
        company = ch.get_company(company_number=number)
    except HTTPError as e:
        print("Companies House API returned error %sn " % str(e))  # Sometimes companies house returns 502
        sleep(15)  # we ill just wait 15 seconds and than retry
        company = ch.get_company(company_number=number)
        if not company:
            res = None
    if company:  # checking for empty dict
        creation_date = datetime.datetime.strptime(company.get("date_of_creation"), "%Y-%m-%d").date()
        time_delta = (datetime.datetime.now().date() - creation_date).days
        print("Company was registered " + str(time_delta) + " days ago")
        if company.get("company_status") == "active" and "registered_office_address" in company and company.get(
                    'type') == "ltd":
            director = get_director(number, ch)
            name = company["company_name"]
            if director:

                address, country, city, postal_code = get_address(company)
                print(name)
                print(director)
                print(address)
                print(number)
                res = [[str(name).replace(',', ' '),
                         str(director).replace(',', ' '),
                         str(address).replace(',', ' '),
                         str(country).replace(',', ' '),
                         str(city).replace(',', ' '),
                         str(postal_code).replace(',', ' ')]]
                return res
    else:
        res = -1
    print(str(number) + " company does not exist or meet our requirements")
    return res


# ------------------------------------------------------------------------------


def main():
    args = get_args()
    ch = CompaniesHouseAPI(_API_KEY, int(args.ratelimit))
    _LAST_NUM_SC = 0
    _LAST_NUM_BR = 0
    empty_counter = 0
    empty_limit = int(args.empty_limit)
    with open(args.last_file, 'r+') as last_file:
        data = json.load(last_file)
        _LAST_NUM_BR = int(data["british_company_last_number"])
        _LAST_NUM_SC = int(data["scottish_company_last_number"])

        # British companies
        with open(args.result_file, "a+", newline='') as res:
            res.write("Company, Fullname, Address, Country, City, Postal Code\n")
            writer = csv.writer(res)
            while True:
                _LAST_NUM_BR += 1
                details = get_company_details(_LAST_NUM_BR, ch)
                print (details)
                if not details:  # happens only if API returned http error or company doesn't meet our requirements
                    continue
                if details == -1:
                    print ("Empty counter 1 " + str(empty_counter))

                    if empty_counter == empty_limit:
                        _LAST_NUM_BR = _LAST_NUM_BR - 1
                        print ("Empty counter 2  " + str(empty_counter))
                        break
                    else:
                        empty_counter += 1
                        continue
                empty_counter = 0
                writer.writerows(details)

            # Scottish companies
            empty_counter = 0
            while True:
                _LAST_NUM_SC += 1
                details = get_company_details("SC" + str(_LAST_NUM_SC), ch)
                if not details:
                    continue
                if details == -1:
                    if empty_counter == empty_limit:
                        _LAST_NUM_SC = _LAST_NUM_SC - 1
                        break
                    else:
                        empty_counter += 1
                        continue
                empty_counter = 0
                writer.writerows(details)
        data["british_company_last_number"] = _LAST_NUM_BR - empty_limit  # because we are checking 100 extra numbers
        data["scottish_company_last_number"] = _LAST_NUM_SC - empty_limit
        last_file.seek(0)
        last_file.truncate()
        json.dump(data, last_file)
        exit(0)

# ------------------------------------------------------------------------------


if __name__ == "__main__":
    sys.exit(main())
