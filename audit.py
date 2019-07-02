from db import upsert
from config import config
from datetime import datetime, timedelta
from pytz import timezone

from api import call_and_load_json

from simple_salesforce import Salesforce
sf = Salesforce(username=config.get('USERNAME'), password=config.get('PASSWORD'), security_token=config.get('TOKEN'))


def escape_email(email):
    # SF doesn't allow certain characters unescaped in email searches.
    # This isn't all of them, so may need to add more.
    return email.replace('-', '\-').replace('+', '\+')


# change days to adjust date range
def create_filters(days=365):
    filters = [('system', 'true')]

    sync_date = datetime.today()

    # get data from 12 hours before the last sync date until now
    sync_date = sync_date - timedelta(days=int(days))

    # Convert to the Wufoo API req.
    # Lets datetime know that our date is central
    localized = timezone('US/Central').localize(sync_date)
    # Translates US/Central to Pacific TZ
    new_date = localized.astimezone(timezone('US/Pacific'))

    # Convert to ISO format
    # YYYY-MM-DD HH:MM:SS
    sync_date = new_date.isoformat()
    # add filters to get new AND updated entries
    filters.extend([('Filter1',
                     'DateUpdated Is_after %s' % sync_date),
                    ('Filter2',
                     'DateCreated Is_after %s' % sync_date),
                    ('match', 'OR')
                    ])
    return filters


def get_entries_for_form(hash):
    page_start = 0
    page_step = 100
    all_entries = []
    while 1:
        # copy filter list so the paging can be appended
        runfilters = create_filters()
        runfilters.extend([
            ('pageStart', page_start),
            ('pageSize', page_step)
        ])
        entries = call_and_load_json(hash, api='entries', extra_params=runfilters)
        try:
            entries = entries['Entries']
        except KeyError:
            print('%s: 502 error - skipping sync' % hash)
            entries = []
        print("%s: found  %s entries" % (hash, len(entries)))
        all_entries.append(entries)

        # break if the # entries is less than our pageStep (i.e. the max #
        # entries returned), since we know there are no more entries after
        # pageStart+pageStep.
        if len(entries) < page_step - 1:
            break
        page_start += page_step

    return all_entries


def search_for_contact_id(email):
    results = sf.quick_search(escape_email(email))
    for result in results.get('searchRecords'):
        if result.get('attributes').get('type') == 'Contact':
            return result.get('Id')
        elif result.get('attributes').get('type') == 'Interaction__c':
            result = sf.query("SELECT Contact__c FROM Interaction__c WHERE Id = '%s'" % result.get('Id'))
            return result.get('records')[0].get('Contact__c')
        elif result.get('attributes').get('type') == 'Task':
            result = sf.query("SELECT WhoId FROM Task WHERE Id = '%s'" % result.get('Id'))
            # not all Tasks are related to a WhoId
            contact_id = result.get('records')[0].get('WhoId')
            if contact_id:
                return contact_id


def find_contact_by_email(email):
    # Ideally we'll find a contact with this exact email
    result = sf.query("SELECT Id, Email FROM Contact WHERE Email = '%s'" % email)
    if result.get('totalSize') == 0:
        # if not, search for a record of this email in a few other objects
        return search_for_contact_id(email)
    else:
        return result.get('records')[0].get('Id')


def sync_form_entries(hash, field):

    entries = get_entries_for_form(hash)
    for segment in entries:
        for entry in segment:
            email = entry.get(field)
            contact_id = find_contact_by_email(email)
            data = {
                'id': '%s-%s' % (hash, entry.get('EntryId')),
                'hash': hash,
                'entryid': entry.get('EntryId'),
                'email': email,
                'contactid': contact_id
            }

            print(data)
            upsert(data)


# forms are in the format like this, where hash is the Wufoo Hash for that form, and Field123 is the API name of the
# Corresponding Email field.
# forms = [
#     ['hash1', 'Field123'],
#     ['hash2', 'Field123'],
# ]

forms = config.get('FORMS')

for form in forms:
    sync_form_entries(form[0], form[1])

