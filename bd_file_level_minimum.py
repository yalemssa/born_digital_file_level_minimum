#/usr/bin/python3

import csv
import json
import requests
import traceback
from concurrent.futures import ThreadPoolExecutor

'''
Script to create or update archival objects according to YUL BDAWG Born-Digital File Level Minumum Standards
'''

def login(url=None, username=None, password=None):
    """Logs into the ArchivesSpace API."""
    try:
        if url is None and username is None and password is None:
            url = input('Please enter the ArchivesSpace API URL: ')
            username = input('Please enter your username: ')
            password = input('Please enter your password: ')
        auth = requests.post(url+'/users/'+username+'/login?password='+password).json()
        #if session key is returned then login was successful; if not it failed.
        if 'session' in auth:
            session = auth["session"]
            headers = {'X-ArchivesSpace-Session':session, 'Content_Type': 'application/json'}
            print('Login successful!')
            return (url, headers)
        else:
            print('Login failed! Check credentials and try again.')
            #try again
            api_url_retry, headers_retry = login()
            return api_url_retry, headers_retry
    except:
        print('Login failed! Check credentials and try again!')
        api_url_retry, headers_retry = login()
        return api_url_retry, headers_retry

def opencsvdict(input_csv=None):
    """Opens a CSV in DictReader mode."""
    try:
        if input_csv is None:
            input_csv = input('Please enter path to CSV: ')
        if input_csv == 'quit':
            quit()
        file = open(input_csv, 'r', encoding='latin1')
        csvin = csv.DictReader(file)
        return csvin
    except:
        print('CSV not found. Please try again. Enter "quit" to exit')
        csvinfile = opencsvdict()
        return csvinfile

def create_backups(dirpath, uri, record_json):
    with open(f"{dirpath}/{uri[1:].replace('/','_')}.json", 'a', encoding='utf8') as outfile:
        json.dump(record_json, outfile, sort_keys=True, indent=4)

def encodeit(row):
    for key, value in row.items():
        value = bytes(value, encoding='utf-8')
        value.decode('latin1').encode('utf-8')
        row[key] = value.decode('utf-8')
    return row

class FileLevelMin():
    def __init__(self):
        self.config = json.load(open('config.json', encoding='utf-8'))
        self.csvfile = opencsvdict(self.config['input_csv'])
        self.dirpath = self.config['output_folder']
        self.api_url, self.headers = login(self.config['api_url'], self.config['username'], self.config['password'])

    def get_object(self, uri, sesh):
        record_json = sesh.get(f"{self.api_url}{uri}", headers=self.headers).json()
        create_backups(self.dirpath, uri, record_json)
        return record_json

    def post_updated_object(self, record_json, csv_row, sesh):
        ao_uri = csv_row.get('archival_object_uri')
        record_update = sesh.post(f"{self.api_url}{ao_uri}", json=record_json, headers=self.headers).json()
        if record_update.get('error') == {'db_error': ['Database integrity constraint conflict: Java::ComMysqlJdbcExceptionsJdbc4::MySQLTransactionRollbackException: Deadlock found when trying to get lock; try restarting transaction']}:
            print(f'Deadlock found. Retrying {ao_uri}...')
            record_json = self.get_object(ao_uri, sesh)
            record_json = self.update_archival_object(csv_row, record_json, sesh)
            record_update = sesh.post(f"{self.api_url}{ao_uri}", json=record_json, headers=self.headers).json()
        elif record_update.get('error') == 'The record you tried to update has been modified since you fetched it.':
            print(f"Modified since fetched: {csv_row.get('archival_object_uri')}")
        else:
            return record_update

    def post_new_object(self, record_json, csv_row, sesh):
        return sesh.post(f"{self.api_url}/repositories/12/archival_objects", json=record_json, headers=self.headers).json()

    def update_archival_object(self, csv_row, record_json, sesh):
        try:
            if csv_row['title'] != '':
                record_json['title'] = csv_row['title']
            record_json = self.create_extents(csv_row, record_json)
            record_json = self.create_date(csv_row, record_json)
            record_json = self.create_scope_note(csv_row, record_json)
            record_json = self.create_arrangement_note(csv_row, record_json)
            record_json = self.create_processinfo_note(csv_row, record_json)
            record_json = self.create_otherfindaid_note(csv_row, record_json)
            record_json = self.create_access_note(csv_row, record_json, sesh)
            return record_json
        except Exception:
            print(traceback.format_exc())

    def create_archival_object(self, csv_row, sesh):
        try:
            record_json = {'jsonmodel_type': 'archival_object',
            'publish': True,
            'resource': {'ref': csv_row['resource']}, 'repository': {'ref': csv_row['repository']},
            'parent': {'ref': csv_row['archival_object_uri']},
            'level': 'file',
            'extents': [],
            'instances': [],
            'dates': [],
            'notes': []}
            record_json = self.update_archival_object(csv_row, record_json, sesh)
            return record_json
        except Exception:
            print(traceback.format_exc())

    def run_update_funcs(self, csv_row, sesh):
        try:
            record_uri = csv_row.get('archival_object_uri')
            record_json = self.get_object(record_uri, sesh)
            record_json = self.update_archival_object(csv_row, record_json, sesh)
            posted_object = self.post_updated_object(record_json, csv_row, sesh)
            print(posted_object)
        # except UnicodeDecodeError:
        #     csv_row = handle_encoding_errors(csv_row)
        #     self.run_update_funcs(csv_row, sesh)
        except Exception:
            print(csv_row)
            print(traceback.format_exc())

    def run_create_funcs(self, csv_row, sesh):
        try:
            record_json = self.create_archival_object(csv_row, sesh)
            posted_object = self.post_new_object(record_json, csv_row, sesh)
            print(posted_object)
        except Exception:
            print(traceback.format_exc())

    def select_operation(self):
        '''This allows a user to select a create or update option'''
        csv_filename = self.config['input_csv']
        if 'create' in csv_filename:
            return self.run_create_funcs
        elif 'update' in csv_filename:
            return self.run_update_funcs
        else:
            #THIS WORKS, but could also mask another error
            raise Exception('Filename does not contain a valid create or update statement')

    def execute_process(self):
        self.operation = self.select_operation()
        try:
            #switched these around to try and prevent the Pool from closing unexpectedly
            with requests.Session() as sesh:
                with ThreadPoolExecutor(max_workers=4) as pool:
                    for row in self.csvfile:
                        pool.submit(self.operation, encodeit(row), sesh)
        except Exception:
            print(traceback.format_exc())

    def create_multipart_note(self, record_json, note_text, note_type, **kwargs):
        '''creates a multipart note'''
        try:
            note_types = tuple(note.get('type') for note in record_json['notes'])
            new_note = {'jsonmodel_type': 'note_multipart', 'type': note_type, 'publish': True,
                            'subnotes': [{'jsonmodel_type': 'note_text', 'content': note_text, 'publish': True}]}
            if note_type == 'accessrestrict':
                new_note = self.create_machine_actionable_restriction(new_note, kwargs)
            if note_type not in note_types:
                record_json['notes'].append(new_note)
            elif note_type in note_types:
                if note_types.count(note_type) < 2:
                    for note in record_json['notes']:
                        if note['type'] == note_type:
                            note['subnotes'][0]['content'] = note_text
                else:
                    record_json['notes'].append(new_note)
            return record_json
        except Exception:
            print(traceback.format_exc())

    def create_date(self, csv_row, record_json):
        '''because there can be multiple dates, first want to find out
        whether the record already has a match. If so then do nothing.
        If there is no match, then append. This is an extra precaution - 
        it is unlikely that born-digital materials will have multiple date
        records - but it is possible'''
        try:
            if csv_row['date_label'] != '' and csv_row['date_type'] != '':
                new_date = {'jsonmodel_type': 'date', 'expression': csv_row['date_expression'], 'begin': csv_row['date_begin'],
                            'date_type': csv_row['date_type'], 'label': csv_row['date_label']}
                if csv_row['date_end'] != '':
                    new_date['end'] = csv_row['date_end']
                if len(record_json['dates']) in (0, 1):
                    record_json['dates'] = [new_date]
                if len(record_json['dates']) > 1:
                    record_json['dates'].append(new_date)
            return record_json
        except Exception:
            print(traceback.format_exc())

    def create_machine_actionable_restriction(self, new_note, kwargs):
        '''What happens if there are multiple access restriction types'''
        try:
            new_note['rights_restriction'] = {}
            for key, value in kwargs.items():
                if key == 'end' and value != '':
                    new_note['rights_restriction']['end'] = value
                if key == 'begin' and value != '':
                    new_note['rights_restriction']['begin'] = value
                if key == 'local_access_restriction_type' and value != '':
                    new_note['rights_restriction']['local_access_restriction_type'] = value
            return new_note
        except Exception:
            print(traceback.format_exc())

    def create_extents(self, csv_row, record_json):
        try:
            if (csv_row['extent_number_1'] != '' and csv_row['extent_portion_1'] != '' and csv_row['extent_type_1'] != ''):
                record_json = self.new_extent(record_json, csv_row['extent_number_1'], csv_row['extent_portion_1'], csv_row['extent_type_1'], csv_row['extent_container_summary_1'], 0)
            if (csv_row['extent_number_2'] != '' and csv_row['extent_portion_2'] != '' and csv_row['extent_type_2'] != ''):
                record_json = self.new_extent(record_json, csv_row['extent_number_2'], csv_row['extent_portion_2'], csv_row['extent_type_2'], csv_row['extent_container_summary_2'], 1)
            return record_json
        except Exception:
            print(traceback.format_exc())

    def new_extent(self, record_json, extent_number, extent_portion, extent_type, extent_container_summary, index_position):
        try:
            new_extent = {'jsonmodel_type': 'extent', 'number': extent_number,
                    'portion': extent_portion, 'extent_type': extent_type}
            if extent_container_summary != '':
                new_extent['container_summary'] = extent_container_summary
            if len(record_json['extents']) == 0:
                record_json['extents'].append(new_extent)
            elif len(record_json['extents']) > 0:
                if index_position == 0:
                    record_json['extents'][0] = new_extent
                if (len(record_json['extents']) == 1 and index_position == 1):
                    record_json['extents'].insert(1, new_extent)
                if (len(record_json['extents']) > 1 and index_position == 1):
                    record_json['extents'][1] = new_extent
            return record_json
        except Exception:
            print(traceback.format_exc())

    def create_access_note(self, csv_row, record_json, sesh):
        try:
            if (csv_row['use_standard_access_note'] != 'Y' and csv_row['access_restrict'] != ''):
                record_json = self.create_multipart_note(record_json, csv_row['access_restrict'], 'accessrestrict', end=csv_row['timebound_restriction_end_date'], begin=csv_row['timebound_restriction_begin_date'], local_access_restriction_type=[csv_row['machine_actionable_restriction_type']])
            elif csv_row['use_standard_access_note'] == 'Y':
                note_text, mar_type = self.standard_access_note(record_json, sesh)
                record_json = self.create_multipart_note(record_json, note_text, 'accessrestrict', local_access_restriction_type=mar_type)
            return record_json
        except Exception:
            print(traceback.format_exc())

    def get_multiple_titles(self, instances, sesh):
        try:
            combined = []
            for instance in instances:
                dig_object_json = self.get_object(instance, sesh)
                combined.append(dig_object_json['title'])
            combined.sort()
            return f"{combined[0]}-{combined[-1]}"
        except Exception:
            print(traceback.format_exc())

    def get_do_instances(self, record_json, sesh):
        try:
            instances = record_json.get('instances')
            if instances:
                digital_object_instances = []
                for instance in instances:
                    if 'digital_object' in instance:
                        digital_object_instances.append(instance['digital_object']['ref'])
                if len(digital_object_instances) == 1:
                    dig_object_json = self.get_object(digital_object_instances[0], sesh)
                    return dig_object_json['title']
                elif len(digital_object_instances) > 1:
                    return self.get_multiple_titles(digital_object_instances, sesh)
                #if the length is 0 it should return None
        except Exception:
            print(traceback.format_exc())

    # def get_ancestors(self, record_json, sesh):
    #     #make sure to use a .get here so that it will work with the create function
    #     ancestors = record_json.get('ancestors')
    #     if ancestors:
    #         for ancestor in ancestors:
    #             if ancestor.get('level') == 'series':
    #                 ancestor_json = self.get_object(ancestor.get('ref'), sesh)
    #                 return ancestor_json.get('component_unique_id')
        #if there is no ancestor list or if there is no CUID it should return None


    def get_digital_object_title(self, record_json, sesh):
        try:
            digital_objects = self.get_do_instances(record_json, sesh)
            if digital_objects:
                return digital_objects
            cuid = record_json.get('component_id')
            if cuid:
                return cuid
        except Exception:
            print(traceback.format_exc())
        
    def standard_access_note(self, record_json, sesh):
        try:
            digital_object_title = self.get_digital_object_title(record_json, sesh)
            if digital_object_title is not None:
                standard_text = f"""As a preservation measure, original materials may not be used. Digital access copies must be provided for use. Contact Manuscripts and Archives at <ref actuate="onRequest" show="new" href="mailto:mssa.assist@yale.edu?subject=Digital Copy Request: {digital_object_title}">mssa.assist@yale.edu</ref> to request access"""
            elif digital_object_title is None:
                standard_text = f"""As a preservation measure, original materials may not be used. Digital access copies must be provided for use. Contact Manuscripts and Archives at <ref actuate="onRequest" show="new" href="mailto:mssa.assist@yale.edu?subject=Digital Copy Request">mssa.assist@yale.edu</ref> to request access"""
            return standard_text, ['RestrictedFragileSpecColl']
        except Exception:
            print(traceback.format_exc())

    def create_arrangement_note(self, csv_row, record_json):
        if csv_row['arrangement'] != '':
            record_json = self.create_multipart_note(record_json, csv_row['arrangement'], 'arrangement')
        return record_json

    def create_processinfo_note(self, csv_row, record_json):
        if csv_row['process_info'] != '':
            record_json = self.create_multipart_note(record_json, csv_row['process_info'], 'processinfo')
        return record_json

    def create_otherfindaid_note(self, csv_row, record_json):
        if csv_row['other_find_aid'] != '':
            record_json = self.create_multipart_note(record_json, csv_row['other_find_aid'], 'otherfindaid')
        return record_json

    def create_scope_note(self, csv_row, record_json):
        if csv_row['scope_content'] != '':
            record_json = self.create_multipart_note(record_json, csv_row['scope_content'], 'scopecontent')
        return record_json


def main():
    file_level_min = FileLevelMin()
    file_level_min.execute_process()
        

if __name__ == "__main__":
    main()