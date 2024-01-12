from fileinput import filename
import pandas as pd
from datetime import datetime, timedelta
import pytz
import email
import io
import argparse
from google.oauth2 import service_account
from utils import unzip_csv , send_email, get_sheet
import csv
from urllib.request import urlopen
import configparser



class Trade_Summary:    

    def __init__(self, start_date, end_date, config) -> None:
        self.start_date = start_date
        self.end_date = end_date
        self.from_email = config['Default']['from_email']
        self.from_email_password = config['Default']['from_email_password']
        self.to_email = config['Default']['to_email']
    
    def __load_sheets(self):
        pl_sheet, pl_df = get_sheet(spreadsheet='PLanalysis', sheet_num=0)
        details_sheet, details_df = get_sheet(spreadsheet='PLanalysis', sheet_num=1)
        data_sheet, data_df = get_sheet(spreadsheet='PLanalysis', sheet_num=-1)
        return pl_sheet, pl_df, details_sheet, details_df, data_sheet, data_df
    
    def __cleaning(self, data_df):
        # cleaning
        data_df['Quantity'] = data_df['Quantity'].astype(int)
        data_df['Date'] = pd.to_datetime(data_df['Date'], format='%d-%m-%Y')
        data_df['Trade Time'] = pd.to_datetime(data_df['Trade Time'], format='%H:%M:%S')
        data_df['Amount'] = data_df['Amount'].replace('[\₹,]', '', regex=True).astype(float)
        data_df['Price'] = data_df['Price'].replace('[\₹,]', '', regex=True).astype(float)
        return data_df

    def __filter_data(self, result_df):
        # oldest order for each company
        min_dates = result_df.groupby('Company')['Date'].idxmin()
        result_df = result_df.loc[~(result_df.index.isin(min_dates) & (result_df['Side']=='Sell'))]
        # oldest order for each company
        max_dates = result_df.groupby('Company')['Date'].idxmax()
        result_df = result_df.loc[~(result_df.index.isin(max_dates) & (result_df['Side']=='Buy'))]

        # companies with single record/row
        single_record = result_df.groupby('Company').size()[result_df.groupby('Company').size() < 2].index.to_list()
        result_df = result_df[~result_df.Company.isin(single_record)]

        # companies with first sell order quantity greater than first buy order quantity
        temp_df = result_df.groupby(['Company','Side'])['Quantity'].min().unstack()
        result_df = result_df[~result_df.Company.isin(temp_df[temp_df['Buy'] < temp_df['Sell']].index.to_list())]

        result_df['Revenue'] = result_df.apply(lambda x: -x['Amount'] if x['Side']=='Buy' else x['Amount'], axis=1)
        return result_df


    def run(self):
        today_date = datetime.today().date().strftime('%d-%m-%Y')                               #today's date
        time_now = datetime.now(pytz.timezone('Asia/Kolkata')).time().strftime('%H:%M:%S')      #time now
        
        try:
            asd
            pl_sheet, pl_df, details_sheet, details_df, data_sheet, data_df = self.__load_sheets()
            data_df = self.__cleaning(data_df)
            result_df = data_df[(data_df['Date'] >= start_date) & (data_df['Date'] <= end_date)].copy()
            result_df = result_df[result_df.Segment=='EQ'].groupby(['Company', 'Side', 'Date']).agg({'Amount': 'sum', 'Quantity': 'sum', 'Price': 'mean'})
            result_df = result_df.reset_index()
            result_df = result_df.sort_values(by=['Company', 'Date'], ascending=[True, False])

            # updating details sheet
            temp_df = result_df.copy().astype(str)
            details_sheet.update([temp_df.columns.values.tolist()] + temp_df.values.tolist())

            #filtering data
            result_df = self.__filter_data(result_df)
            net_p = result_df.Revenue.sum()
            data_id = data_sheet.title
            row = [str(data_id), str(today_date), str(time_now), str(net_p)]
            # updating pl_logs
            pl_df = pl_df.astype(stre)
            pl_sheet.update([pl_df.columns.values.tolist()] + [row] + pl_df.values.tolist())
        
        except Exception as e:
            # send an email if there is an error in updating
            send_email(
                to_email=self.to_email,
                subject='Error in Updating Data', 
                body_text=f'There was an error in updating the analysis : \n{e}'
                )
            print('--Error--\n')
            print(e)


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('variables.ini')
    parser = argparse.ArgumentParser(description="Upload Proft/Loss analysis to google sheet.")
    parser.add_argument(
        "--days",
        type=int,
        default=30
    )
    args = parser.parse_args()
    days = args.days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    print(start_date.date(), end_date.date())
    trs = Trade_Summary(
        start_date=start_date,
        end_date=end_date,
        config=config
        )
    print('Running...')
    trs.run()
    print('Done.')