#!/usr/bin/env python
DESC="""Module called by other EPP scripts to write notes to couchdb
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
        email_error('Statusdb credentials not found in {}\n '.format(lims), 'genomics-bioinfo@scilifelab.se')
        email_error('Running note save for {} failed on LIMS! Please contact {} to resolve the issue!'.format(pid, 'genomics-bioinfo@scilifelab.se'), note['email'])
        sys.exit(1)
    url_string = 'http://{}:{}@{}:{}'.format(config['statusdb'].get('username'), config['statusdb'].get('password'),
                                              config['statusdb'].get('url'), config['statusdb'].get('port'))
    couch = couchdb.Server(url=url_string)
    if not couch:
        email_error('Connection failed from {} to {}'.format(lims, config['statusdb'].get('url')), 'genomics-bioinfo@scilifelab.se')
        email_error('Running note save for {} failed on LIMS! Please contact {} to resolve the issue!'.format(pid, 'genomics-bioinfo@scilifelab.se'), note['email'])

    proj_db = couch['projects']
    v = proj_db.view('project/project_id')
    if len(v[pid]) == 0:
        msg = 'Project {} does not exist in {} when syncing from {}\n '.format(pid, config['statusdb'].get('url'), lims)
        for user_email in ['genomics-bioinfo@scilifelab.se', note['email']]:
            email_error(msg, user_email)
    else
        for row in v[pid]:
            doc_id = row.value
        doc = proj_db.get(doc_id)
        running_notes = doc['details'].get('running_notes', '{}')
        running_notes = json.loads(running_notes)

        running_notes.update({timestamp: note})
        doc['details']['running_notes'] = json.dumps(running_notes)
        proj_db.save(doc)
        #check if it was saved
        doc = proj_db.get(doc_id)
        if doc['details']['running_notes'] != json.dumps(running_notes):
            msg = 'Running note save failed from {} to {} for {}'.format(lims, config['statusdb'].get('url'), pid)
            for user_email in ['genomics-bioinfo@scilifelab.se', note['email']]:
                email_error(msg, user_email)

def email_error(msg, resp_email):
    body = 'Error: '+msg
    body += '\n\n--\nThis is an automatically generated error notification'
    msg = MIMEText(body)
    msg['Subject'] = '[Error] Running note sync error from LIMS to Statusdb'
    msg['From'] = 'Lims_monitor'
    msg['To'] = resp_email


    s = smtplib.SMTP('localhost')
    s.sendmail('genologics-lims@scilifelab.se', msg['To'], msg.as_string())
    s.quit()
