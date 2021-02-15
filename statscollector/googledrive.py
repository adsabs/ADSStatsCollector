import os
import re
import tempfile
from datetime import datetime
from dateutil.relativedelta import relativedelta as tsdelta
from apiclient.discovery import build
from apiclient import errors
from httplib2 import Http
from oauth2client import file, client, tools
from .setup import config, logger

def verify_access():
    if os.path.exists(config.get('GOOGLE_DRIVE_CREDENTIALS_FILENAME')):
        # Setup the Drive v3 API
        # - Create a project in https://console.developers.google.com/
        # - Create a credential for the project (type OAuth 2.0 Client ID, application type: Desktop app)
        # - Download the credentials and store them as 'credentials.json' in the app root folder
        # - Run the verify access
        store = file.Storage(config.get('GOOGLE_DRIVE_TOKEN_FILENAME'))
        creds = store.get()
        if not creds or creds.invalid:
            message = "Please, authorize access to the Google Team Drive with your CfA account (your browser should open to do so)."
            logger.info(message)
            #scopes = ['https://www.googleapis.com/auth/drive'] # View and manage files all your files
            scopes = ['https://www.googleapis.com/auth/drive.file'] # View and manage files created with this app
            flow = client.flow_from_clientsecrets('credentials.json', scopes)
            args = tools.argparser.parse_args(args=['--noauth_local_webserver'])
            creds = tools.run_flow(flow, store, args)
        return creds
    else:
        message = "No credentials file found. Please, follow the instructions in https://developers.google.com/drive/api/v3/quickstart/python to download the file 'credentials.json'. Meanwhile files cannot be uploaded to Google Team Drive."
        logger.error(message)
        return None


def upload(batch, keep_last_n_folders=30):
    parent_folder_id = config.get('GOOGLE_DRIVE_FOLDER_ID')
    folder_id = _get_or_create_today_folder_id(parent_folder_id)
    _upload_batch(folder_id, batch)
    _keep_only_last_n_folders(parent_folder_id, keep_last_n_folders, folder_name_regex="^\d{8}$")


def _get_or_create_today_folder_id(parent_folder_id):
    now = datetime.utcnow()
    folder_name = "{:04}{:02}{:02}".format(now.year, now.month, now.day)
    folder_id = _mkdir(parent_folder_id, folder_name, avoid_duplicate_name=True)
    return folder_id

def _upload_batch(parent_folder_id, batch):
    for name, bibcodes in batch.items():
        fd, path = tempfile.mkstemp(suffix=".txt")
        try:
            with os.fdopen(fd, 'w') as tmp:
                for bibcode in bibcodes:
                    tmp.write("%s\n" % bibcode)
            _upload(parent_folder_id, name+".txt", path)
        except:
            logger.exception("Unable to upload text file '%s' to Google Team Drive", name)
        finally:
            os.remove(path)

def _keep_only_last_n_folders(parent_folder_id, keep_last_n_folders, folder_name_regex="^\d{8}$"):
    # Keep only last N folders that match format YYYYMMDD (ignore if they do not match)
    existing_folders = _ls(parent_folder_id, mimetype="application/vnd.google-apps.folder", order_by="modifiedTime") # oldest files at the end
    n_keep = 0
    for existing_folder in existing_folders:
        if folder_name_regex and not re.match("^\d{8}$", existing_folder.get('name')):
            continue
        if n_keep > keep_last_n_folders:
            _rm(existing_folder.get('id'))
        else:
            n_keep += 1

def _upload(parent_folder_id, file_name, file_path, mimetype='text/plain'):
    file_id = None
    if os.path.exists(config.get('GOOGLE_DRIVE_TOKEN_FILENAME')) and os.path.exists(file_path):
        store = file.Storage(config.get('GOOGLE_DRIVE_TOKEN_FILENAME'))
        creds = store.get()
        try:
            drive_service = build('drive', 'v3', http=creds.authorize(Http()))
            body = {'name': file_name, 'mimeType': mimetype, 'parents': [parent_folder_id]}
            created_file = drive_service.files().create(body=body, media_body=file_path, fields='id, parents', supportsTeamDrives=True).execute()
            file_id = created_file['id']
            file_url = "https://drive.google.com/file/d/{}".format(file_id)
            folder_url = "https://drive.google.com/drive/u/1/folders/{}".format(parent_folder_id)
            logger.debug("File '%s' uploaded to '%s' in folder '%s", file_name, file_url, folder_url)
        except:
            logger.exception("File '%s' failed to be uploaded to Google Team Drive", file_name)
    else:
        logger.error("File '%s' failed to be uploaded to Google Team Drive", file_name)
    return file_id


def _mkdir(parent_folder_id, folder_name, avoid_duplicate_name=False):
    folder_id = None
    if avoid_duplicate_name:
        # Search if a directory already exists with the same name
        existing_folders = _ls(parent_folder_id, mimetype="application/vnd.google-apps.folder")
        found = None
        for existing_folder in existing_folders:
            if existing_folder.get('name') == folder_name:
                found = existing_folder
                break
        if found:
            folder_id = found.get('id')
    if folder_id is None:
        folder_id = _true_mkdir(parent_folder_id, folder_name)
    if folder_id is None:
        logger.error("Failed to create folder in Google Drive Team")
    return folder_id


def _true_mkdir(parent_folder_id, folder_name):
    folder_id = None
    if os.path.exists(config.get('GOOGLE_DRIVE_TOKEN_FILENAME')):
        store = file.Storage(config.get('GOOGLE_DRIVE_TOKEN_FILENAME'))
        creds = store.get()
        try:
            drive_service = build('drive', 'v3', http=creds.authorize(Http()))
            folder_id = config.get('GOOGLE_DRIVE_FOLDER_ID')
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [folder_id]
            }
            folder_id = drive_service.files().create(body=file_metadata, fields='id', supportsTeamDrives=True).execute()
        except:
            logger.exception("Failed to create folder '%s' in Google Team Drive", folder_name)
    else:
        logger.error("Failed to create folder '%s' in Google Team Drive", folder_name)
    return folder_id


def _ls(folder_id, older_than=None, mimetype=None, order_by="modifiedTime"):
    """
    Retrieve a list of File resources.

    Source: https://github.com/paulknewton/google-drive-cleaner/

    :param service: Google Drive API service instance
    :param folder_id: Google Drive reference to the folder to be queried (cleaned)
    :return a list of File resources
    """
    result = []
    if os.path.exists(config.get('GOOGLE_DRIVE_TOKEN_FILENAME')):
        store = file.Storage(config.get('GOOGLE_DRIVE_TOKEN_FILENAME'))
        creds = store.get()
        try:
            drive_service = build('drive', 'v3', http=creds.authorize(Http()))
            params = {
                "q": "'" + folder_id + "' in parents and trashed=false",
                "orderBy": order_by,
                "fields": "nextPageToken, files(id, name, mimeType)",
                "supportsAllDrives": True,
                "includeItemsFromAllDrives": True,
            }
            if older_than:
                # Example:
                # older_than = (datetime.utcnow() - tsdelta(hours=6)).isoformat("T") + "Z"
                params["q"] += " and modifiedTime < '{}'".format(older_than)
            if mimetype:
                params["q"] += " and mimeType = '{}'".format(mimetype)
            page_token = None
            while True:
                try:
                    if page_token:
                        params['pageToken'] = page_token
                    files = drive_service.files().list(**params).execute()

                    result.extend(files.get('files'))
                    page_token = files.get('nextPageToken')  # more results may be available

                    if not page_token:
                        break
                except errors.HttpError as error:
                    print('An error occurred: %s' % error)
                    break
        except:
            logger.exception("Failed to list content from '%s' in Google Team Drive", folder_id)
    else:
        logger.error("Failed to list content from '%s' in Google Team Drive", folder_id)
    return result


def _rm(file_or_directory_id):
    """
    Delete a file or directory from the Google Drive
    """
    if os.path.exists(config.get('GOOGLE_DRIVE_TOKEN_FILENAME')):
        store = file.Storage(config.get('GOOGLE_DRIVE_TOKEN_FILENAME'))
        creds = store.get()
        try:
            drive_service = build('drive', 'v3', http=creds.authorize(Http()))
            drive_service.files().delete(fileId=file_or_directory_id, supportsAllDrives=True).execute()
        except:
            logger.exception("File/directory '%s' failed to be deleted from Google Team Drive", file_or_directory_id)
    else:
        logger.error("File/directory '%s' failed to be deleted from Google Team Drive", file_or_directory_id)

