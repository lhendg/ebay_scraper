import pandas as pd
import numpy as np
import requests
import re
import os 

from datetime import datetime
from bs4 import BeautifulSoup

#### GOOGLE DRIVE DEPENDENCIES ###
from pydrive.auth import GoogleAuth, ServiceAccountCredentials
from pydrive.drive import GoogleDrive


scope = ['https://www.googleapis.com/auth/drive']
DRIVE_FOLDER = ''
CREDENTIALS = ''

def get_products(STORE_LINK):
    page = BeautifulSoup(requests.get(STORE_LINK).text, 'html.parser')
    item_count = int(re.findall(r'\d+', page.find('h1', class_="srp-controls__count-heading").text)[0])
    PAGE_COUNT = round(np.ceil(item_count/192))

    dataframe = pd.DataFrame()
    
    for i in range(1, PAGE_COUNT+1): 
        LINK = STORE_LINK + "&_pgn=" + str(i)
        response = requests.get(LINK)
        page = BeautifulSoup(response.text, 'html.parser')

        main_grid = page.find('ul', class_='srp-results srp-grid clearfix')

        try:    
            remove = main_grid.find('div', class_='srp-river-answer srp-river-answer--RIGHT_ALIGNED_MESSAGE')
            remove.extract()

            remove1 = main_grid.find('div', class_='srp-river-answer srp-river-answer--BASIC_PAGINATION_V2 ')
            remove1.extract()
        except:
            pass

        the_products = main_grid.find_all('div', class_='s-item__wrapper clearfix')

        products = []

        for k in range(0, len(the_products)):
            product_name = the_products[k].find('div', class_='s-item__image').find('img')['alt'] ## name
            product_link = the_products[k].find('div', class_='s-item__image').find('a')['href'] ## link

            try: 
                product_rating = the_products[k].find('div', class_='x-star-rating').find('span', class_='clipped').text
            except:  
                product_rating = 'No Ratings'
            product_price = the_products[k].find('div', class_='s-item__detail s-item__detail--primary').find('span', class_='s-item__price').text

            product = [product_name, product_link, product_price, product_rating]
            products.append(product)

        df = pd.DataFrame(products, columns = ['product_name', 'link', 'price', 'rating'])

        print("Scraped Products from {0} Page {1} Successfully".format(STORE, i), datetime.now())

        dataframe = pd.concat([dataframe, df]).reset_index(drop=True)
    return dataframe

def get_product_descriptions(df):     
    for link in df['link']: 
        response = requests.get(link)
        page = BeautifulSoup(response.text, 'html.parser')

        try: 
            page.find('span', class_='qtyTxt vi-vpqp-qtyTxt').extract()
        except: 
            pass

        try: 
            item_stock = page.find('span', class_='qtyTxt vi-bboxrev-dsplblk vi-qty-fixAlignment feedbackON vi-vpqp-feedback').find('span', id='qtySubTxt').text
        except:
            pass

        try:
            item_stock = page.find('span', id='qtySubTxt').text
        except:
            pass

        try:
            x = page.find_all('span', id="qtySubTxt")
            for y in x:
                z = i.find('span', class_="")
                item_stock = z.text
        except:
            pass
        
        ITEM_STOCK.append(item_stock)

        keys = []
        row_values = []

        try:
            for a in page.find('table', 'sh-tbl').find_all('th'):
                row = a.text
                row = row.strip()
                keys.append(row)

            for a in page.find('table', 'sh-tbl').find_all('td'):
                row = a.text
                row = row.strip()
                row_values.append(row)    

            delivery_df = pd.DataFrame(row_values, keys, columns=['tbl']).reset_index()

            for b in delivery_df[delivery_df['index'].isin(['Delivery*'])]['tbl']:
                est_delivery = b
        except:
            est_delivery = np.nan
        
        EST_DELIVERY.append(est_delivery)
    
        labels = []
        values = []
        item_desc_df = pd.DataFrame()

        item_desc = page.find('div', id='viTabs_0_cnt')

        try:    
            remove = item_desc.find('span', class_='ux-expandable-textual-display-block-inline hide')
            remove.extract()

            remove1 = item_desc.find('span', class_='clipped')
            remove1.extract()
        except:
            pass

        
        try: 
            for y in item_desc.find_all('div', class_='ux-labels-values__labels-content'):
                z = y.find('span').text
                labels.append(z)

                try:    
                    remove = item_desc.find('span', class_='ux-expandable-textual-display-block-inline hide')
                    remove.extract()

                    remove1 = item_desc.find('span', class_='clipped')
                    remove1.extract()
                except:
                    pass

            for a in item_desc.find_all('div', class_='ux-labels-values__values-content'):
                try: 
                    b = a.find('span')
                    values.append(b.text)
                except:
                    values.append(np.nan)

            item_df = pd.DataFrame(values, labels, columns=['values']).reset_index()
            item_df['index'] = item_df['index'].str.strip()
            item_df.loc[item_df['index'].isin(['Manufacturer Part Number:', 'MPN']), 'index'] = 'MPN:'
            item_df = item_df.drop_duplicates(keep='first')

            if len(item_df[item_df['index'].isin(['MPN:'])]['values']) > 0: 
                for j in item_df[item_df['index'].isin(['MPN:'])]['values']:
                    mpn = j
            else: 
                mpn = np.nan
        except:
            mpn = np.nan

        mpns = [link, mpn]
        MPN.append(mpns)
    
    return print("{} Item Descriptions Successfully Scraped".format(STORE))

def upload_csv(): 
    filename_list = os.listdir("/home/developers/Ebay_Data_Scraper/data")
    
    for i in filename_list:
        file = drive.CreateFile({'title': i, "parents": [{"kind": "drive#fileLink", "id": DRIVE_FOLDER}]})
        file.SetContentFile("/home/developers/Ebay_Data_Scraper/data/{}".format(i))
        file.Upload()
        print(i, "Successfully Uploaded", datetime.now())
        uploaded_time = pd.to_datetime(datetime.now())
        timestamp = [i.strip('.csv'), i, uploaded_time]
        upload_timestamps.append(timestamp)

test = pd.DataFrame(columns=['Cronjob Working'])
test['Cronjob Working'] = datetime.now()
test.to_csv('/home/developers/Ebay_Data_Scraper/test.csv')

gauth = GoogleAuth()
gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS, scope)
drive = GoogleDrive(gauth)

file_list = drive.ListFile({'q': "'1dRZp6ArwEfSn6CrwjlE-G7XEveq1DO0e' in parents and trashed=false"}).GetList()

if file_list==[]: 
    print('No Files in Drive Folder')
    pass
else:
    for file_ in file_list:
        existing_file = drive.CreateFile({'id': file_['id']})
        existing_file.Delete()
        print(file_['title'], "Deleted")

store_dict = dict(pd.read_csv('/home/developers/Ebay_Data_Scraper/monster_stores.csv').values)

for STORE, STORE_LINK in store_dict.items():
    products_df = get_products(STORE_LINK)
    print(STORE, datetime.now())
    
    ITEM_STOCK = []
    EST_DELIVERY = []
    MPN = []

    products_df = products_df.drop_duplicates(keep='first')
    get_product_descriptions(products_df)
    print(STORE, "Products Scraped", datetime.now())
    
    if len(ITEM_STOCK) > 0: 
        products_df['STOCK'] = ITEM_STOCK
    else: 
        products_df['STOCK'] = np.nan
    if len(EST_DELIVERY) > 0:
        products_df['ESTIMATED_DELIVERY'] = EST_DELIVERY
    else: 
        products_df['ESTIMATED_DELIVERY'] = np.nan
    if len(MPN) > 0:
        mpn_df = pd.DataFrame(MPN, columns=['link', 'MPN'])
        products_df = products_df.merge(mpn_df, on='link', how='left')
    else: 
        products_df['MPN'] = np.nan
    
    try:
        products_df['STOCK'] = products_df['STOCK'].str.strip()
        products_df['ESTIMATED_DELIVERY'] = products_df['ESTIMATED_DELIVERY'].str.strip()
        products_df['MPN'] = products_df['MPN'].str.strip()
    except: 
        pass
        
    products_df['loaded_at'] = pd.to_datetime(datetime.now())
    products_df.to_csv('/home/developers/Ebay_Data_Scraper/data/{}.csv'.format(STORE), index=False)

upload_timestamps = []

upload_csv()
print('All Files Uploaded Successfully', datetime.now())