from datetime import datetime, timedelta
import time
import json
import csv
import urllib2

#--------------------------------------

class CSVHelper(object):
    
#--------------------------------------
# Public Functions
#--------------------------------------

    def read(path, key):
        with open(path) as csvfile:
            reader = csv.DictReader(csvfile)
            listItem = []
            for row in reader:
                if row.has_key('email'):
                    listItem.append(row['email'])

            return listItem

        return []

    def write(path, data, fieldnames):
        """
        Writes a CSV file using DictWriter
        """
        with open(path, "wb") as out_file:
            writer = csv.DictWriter(out_file, delimiter=',', fieldnames=fieldnames)
            writer.writeheader()
            for row in data:
                writer.writerow(row)

#--------------------------------------


class FullContactClient(object):
    REQUEST_LATENCY=0.0

    def __init__(self):
        self.next_req_time = datetime.fromtimestamp(0)

    def load_data_for(self, email):
        res = call_fullcontact(email)
        return get_data(res, email)

#--------------------------------------
# Public Helpers Functions
#--------------------------------------

    def call_fullcontact(self, email):
        self._wait_for_rate_limit()

        req = urllib2.Request('https://api.fullcontact.com/v3/person.enrich')
        req.add_header('Authorization', 'Bearer 
                       #Insert key here
                       ')
        data = json.dumps({
            "email": email
        })

        response = urllib2.urlopen(req,data)
        the_page = response.read()
        return json.loads(the_page)

    def get_data(response, email):
        dict_of_data = {}

        dict_of_data['email'] = email

        if response.has_key('fullName'):
            name = response["fullName"]
            dict_of_data['name'] = name.encode('utf-8')

        if response.has_key('linkedin'):
            linkedin = response["linkedin"]
            dict_of_data['linkedin'] = linkedin.encode('utf-8')

        return dict_of_data

#--------------------------------------
# Internal Helper Functions
#--------------------------------------

    def _wait_for_rate_limit(self):
        now = datetime.now()
        if self.next_req_time > now:
            t = self.next_req_time - now
            time.sleep(t.total_seconds())

    def _update_rate_limit(self, hdr):
        remaining = float(hdr['X-Rate-Limit-Remaining'])
        reset = float(hdr['X-Rate-Limit-Reset'])
        spacing = reset / (1.0 + remaining)
        delay = spacing - self.REQUEST_LATENCY
        self.next_req_time = datetime.now() + timedelta(seconds=delay)

#--------------------------------------


#--------------------------------------
# Main Program
#--------------------------------------

def read_and_write():
    api = FullContactClient()
    csv = CSVHelper()
    data = []
    emails = csv.read("input.csv", "email")
    for email in emails:
        dict_of_data = api.load_data_for(email)
        data.append(dict_of_data)

    csv.write("output.csv", data, ['email', 'name', 'linkedin'])
    
read_and_write()
