import os
import tempfile
from apiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
from .setup import config, logger

def verify_access():
    if os.path.exists(config.get('GOOGLE_DRIVE_CREDENTIALS_FILENAME')):
        # Setup the Drive v3 API
        store = file.Storage(config.get('GOOGLE_DRIVE_TOKEN_FILENAME'))
        creds = store.get()
        if not creds or creds.invalid:
            message = "Please, authorize access to the Google Team Drive with your CfA account (your browser should open to do so)."
            logger.info(message)
            scopes = 'https://www.googleapis.com/auth/drive'
            flow = client.flow_from_clientsecrets('credentials.json', scopes)
            args = tools.argparser.parse_args()
            args.noauth_local_webserver = True
            creds = tools.run_flow(flow, store, args)
        return creds
    else:
        message = "Please, follow the instructions in https://developers.google.com/drive/api/v3/quickstart/python to download the file 'credentials.json'. Meanwhile files cannot be uploaded to Google Team Drive."
        logger.error(message)
        return None

def _upload(name, path):
    if os.path.exists(config.get('GOOGLE_DRIVE_CREDENTIALS_FILENAME')) and os.path.exists(config.get('GOOGLE_DRIVE_TOKEN_FILENAME')) and os.path.exists(path):
        store = file.Storage(config.get('GOOGLE_DRIVE_TOKEN_FILENAME'))
        creds = store.get()
        try:
            drive_service = build('drive', 'v3', http=creds.authorize(Http()))
            folder_id = config.get('GOOGLE_DRIVE_FOLDER_ID')
            body = {'name': name, 'mimeType': 'text/plain', 'parents': [folder_id]}
            created_file = drive_service.files().create(body=body, media_body=path, fields='id, parents', supportsTeamDrives=True).execute()
            file_url = "https://drive.google.com/file/d/{}".format(created_file['id'])
            folder_url = "https://drive.google.com/drive/u/1/folders/{}".format(folder_id)
            logger.debug("File '%s' uploaded to '%s' in folder '%s", name, file_url, folder_url)
        except:
            logger.exception("File '%s' failed to be uploaded to Google Team Drive", name)
    else:
        logger.error("File '%s' failed to be uploaded to Google Team Drive", name)


def upload(batch):
    for name, bibcodes in batch.items():
        fd, path = tempfile.mkstemp(suffix=".txt")
        try:
            with os.fdopen(fd, 'w') as tmp:
                for bibcode in bibcodes:
                    tmp.write("%s\n" % bibcode)
            _upload(name+".txt", path)
        except:
            logger.exception("Unable to upload text file '%s' to Google Team Drive", name)
        finally:
            os.remove(path)
