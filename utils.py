import os
import zipfile
import numpy as np
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
import zipfile
import gspread
from oauth2client.service_account import ServiceAccountCredentials



def unzip_csv(file):
    zf = zipfile.ZipFile(file)
    csv_file_name = zf.namelist()[0]
    final_csv_data = zf.open(csv_file_name)
    return final_csv_data

dir_path = os.getcwd()

def saveList(myList,filename):
    # the filename should mention the extension 'npy'
    np.save(filename,myList)
    print("Saved successfully!")



import smtplib
def send_email(from_email, from_email_pass, to_email, subject, body_text):
    #Ports 465 and 587 are intended for email client to email server communication - sending email
    server = smtplib.SMTP('smtp.gmail.com', 587)
    #starttls() is a way to take an existing insecure connection and upgrade it to a secure connection using SSL/TLS.
    server.starttls()
    #Next, log in to the server
    server.login(from_email, from_email_pass)
    msg = MIMEMultipart()
    msg["Subject"] = subject
    body = MIMEText(body_text)
    msg["From"] = from_email
    msg["To"] = to_email
    msg.attach(body)
    server.sendmail(msg["From"], msg["To"],msg.as_string())

#Send the mail


def get_sheet(spreadsheet:str='PLanalysis', sheet_num:int=0):
    #Authorizing the API
    scope = [
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/drive.file',
        'https://spreadsheets.google.com/feeds'
        ]
    key_file = './creds/damascus-sheets-23cd6fa46f09.json'
    creds = ServiceAccountCredentials.from_json_keyfile_name(key_file,scope)
    client = gspread.authorize(creds)
    worksheet = client.open(spreadsheet)
    uploadparms_sheet = worksheet.get_worksheet(sheet_num)
    upload_params_dict = uploadparms_sheet.get_all_records()
    upload_params = pd.DataFrame.from_dict(upload_params_dict)
    # upload_params.set_index('ID', inplace = True)
    # upload_params['Last Run Date'] = pd.to_datetime(upload_params['Last Run Date'], format='%Y-%m-%d')
    return uploadparms_sheet, upload_params


####
