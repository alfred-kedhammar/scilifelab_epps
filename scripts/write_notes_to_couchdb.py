#!/usr/bin/env python
DESC="""Common script called by other EPP scripts to write notes to couchdb
"""
import json
import yaml
import couchdb
import smtplib
import os
from email.mime.text import MIMEText


def write_note_to_couch(pid, timestamp, note, lims):
    configf = '~/.statusdb_cred.yaml'
    with open(os.path.expanduser(configf)) as config_file:
        config = yaml.safe_load(config_file)
    if not config['statusdb']:
        print('Statusdb credentials not found')
        sys.exit(1)
    url_string = 'http://{}:{}@{}:{}'.format(config['statusdb'].get('username'), config['statusdb'].get('password'),
                                              config['statusdb'].get('url'), config['statusdb'].get('port'))
    couch = couchdb.Server(url=url_string)
    if not couch:
        email_error('Connection failed from {} to {}'.format(lims, config['statusdb'].get('url')), 'genomics-bioinfo@scilifelab.se')

    proj_db = couch['projects']
    doc_id = proj_db.view('project/project_id')[pid].rows[0].value
    if not doc_id:
        email_error('Project {} does not exist in {} when syncing from {}\n '.format(pid, config['statusdb'].get('url'), lims), 'genomics-bioinfo@scilifelab.se')
    doc = proj_db.get(doc_id)
    running_notes = doc['details'].get('running_notes', '{}')
    running_notes = json.loads(running_notes)

    running_notes.update({timestamp: note})
    doc['details']['running_notes']=json.dumps(running_notes)
    proj_db.save(doc)
    #check if it was saved
    doc = proj_db.get(doc_id)
    if doc['details']['running_notes'] != json.dumps(running_notes):
        email_error('Running note save failed from {} to {} for {}'.format(lims, config['statusdb'].get('url'), pid), 'genomics-bioinfo@scilifelab.se')

def email_error(msg, resp_email):
    body = 'Error: '+msg
    body += '\n\n--\nThis is an automatically generated error notification'
    msg = MIMEText(body)
    msg['Subject'] = '[Error] Running note sync error from LIMS to Statusdb'
    msg['From']='Lims_monitor'
    msg['To'] = resp_email


    s = smtplib.SMTP('localhost')
    s.sendmail('genologics-lims@scilifelab.se', msg['To'], msg.as_string())
    s.quit()
