import os
import zipfile
import numpy as np
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
import zipfile
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import base64
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from requests import HTTPError
import configparser


config = configparser.ConfigParser()
config.read('variables.ini')


dir_path = os.getcwd()
def unzip_csv(file):
    zf = zipfile.ZipFile(file)
    csv_file_name = zf.namelist()[0]
    final_csv_data = zf.open(csv_file_name)
    return final_csv_data


def saveList(myList,filename):
    # the filename should mention the extension 'npy'
    np.save(filename,myList)
    print("Saved successfully!")


def send_email(to_email, subject, body_text, 
               key_file=config['Default']['email_auth_key']):
    SCOPES = [
            "https://www.googleapis.com/auth/gmail.send"
        ]
    flow = InstalledAppFlow.from_client_secrets_file(key_file, SCOPES)
    creds = flow.run_local_server(port=0)

    service = build('gmail', 'v1', credentials=creds)
    message = MIMEText(body_text)
    message['to'] = to_email
    message['subject'] = subject
    create_message = {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}

    #Send the mail
    try:
        message = (service.users().messages().send(userId="me", body=create_message).execute())
        print(F'sent message to {message} Message Id: {message["id"]}')
    except HTTPError as error:
        print(F'An error occurred: {error}')
        message = None
    


def get_sheet(spreadsheet:str='PLanalysis', sheet_num:int=0, 
              key_file=config['Default']['service_key']):
    #Authorizing the API
    SCOPES = [
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/drive.file',
        'https://spreadsheets.google.com/feeds'
        ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(key_file,SCOPES)
    client = gspread.authorize(creds)
    worksheet = client.open(spreadsheet)
    uploadparms_sheet = worksheet.get_worksheet(sheet_num)
    upload_params_dict = uploadparms_sheet.get_all_records()
    upload_params = pd.DataFrame.from_dict(upload_params_dict)
    # upload_params.set_index('ID', inplace = True)
    # upload_params['Last Run Date'] = pd.to_datetime(upload_params['Last Run Date'], format='%Y-%m-%d')
    return uploadparms_sheet, upload_params


####
