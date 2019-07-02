# python
import base64
import random
import simplejson
import urllib.parse

# venv
import requests
from bs4 import BeautifulSoup

# local
from config import config


def call_api(
        form, api='forms', call_format='json', extra_params=None, unquote_plus=False, request_type='get', payload={}):
    base_url = config['WUFOO_BASE_URL']

    if not form or form == 'all':
        url = base_url + 'api/v3/%s.%s' % (api, call_format)
    else:
        # tests for delete, for use with the webhook API
        if request_type == 'delete':
            url = base_url + 'api/v3/forms/%s/%s/%s.%s' % (form, api, payload.get('webhook_hash'), call_format)
        else:
            # goes through the possible URL for an API of forms
            if api == 'forms':
                # requesting form information for a single form
                # this has a different URL format that the other form apis
                url = base_url + 'api/v3/forms/%s.%s' % (form, call_format)
            elif api == 'count':
                url = base_url + 'api/v3/forms/%s/entries/%s.%s' % (form, api, call_format)
            else:
                url = base_url + 'api/v3/forms/%s/%s.%s' % (form, api, call_format)

    if extra_params:
        params = urllib.parse.urlencode(extra_params)
        if unquote_plus:
            # urlencode uses quote_plus(), when extra_params contains a wufoo filter
            # the plus signs can not be encoded
            params = urllib.parse.unquote_plus(params)
        if api != 'webhooks':
            url += '?' + params

    return load_url(url, request_type, payload)


def load_url(url, request_type, payload):
    keys = config['API_KEYS']
    api_key = random.choice(keys)

    basic_auth_header = base64.b64encode(('%s:%s' % (api_key, 'blastoff')).encode('utf-8'))
    authheader = 'Basic ' + basic_auth_header.decode('utf-8')
    headers = {'Authorization': authheader}
    if request_type == 'put':
        data = {'api_url': url, 'authheader': authheader}
        data.update(payload)
        r = requests.put(url, data=data, headers=headers)
        return r.text
    elif request_type == 'delete':
        r = requests.delete(url, headers=headers)
        return r.text
    else:
        # else statement is leading to a 'get' request
        headers.update({'api_url': url})
        r = requests.get(url, headers=headers, proxies={})
        return r.text


def call_and_load_json(form, api='forms', extra_params=None, unquote_plus=False):
    r = call_api(form, api=api, extra_params=extra_params, unquote_plus=unquote_plus)
    # This was incorrect, there were meant to be entries returned.
    # commenting so we get an error email if it happens again.
    if r == '{"Entries":}':
        # if the result set has no value for Entries, give it an emtpy one.
        r = '{"Entries":""}'
    from simplejson import JSONDecodeError
    try:
        return simplejson.loads(r)
    except TypeError:
        return r
    except JSONDecodeError:
        soup = BeautifulSoup(r, 'html.parser')
        title = soup.title
        if title:
            print(r)
            print(form)
            print(api)
            raise

