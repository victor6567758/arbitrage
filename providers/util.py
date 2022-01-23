import http
import json
import urllib.request
import logging


def read_request_helper(url):
    try:
        request_obj = urllib.request.urlopen(url)
        response_json = ""
        while True:
            try:
                response_json_npart = request_obj.read()
            except http.client.IncompleteRead as icread:
                response_json = response_json + icread.partial.decode('utf-8')
                continue
            else:
                response_json = response_json + response_json_npart.decode('utf-8')
                break

        return json.loads(response_json)

    except Exception as err:
        logging.log(logging.ERROR, "Exception occurred making REST call: %s", err)
