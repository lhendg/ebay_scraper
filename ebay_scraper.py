import pandas as pd
import numpy as np
import requests
import re

from datetime import datetime
from bs4 import BeautifulSoup

BASE_LINK = ""

page = BeautifulSoup(requests.get(BASE_LINK).text, 'html.parser')
item_count = int(re.findall(r'\d+', page.find('h1', class_="srp-controls__count-heading").text)[0])
PAGE_COUNT = round(np.ceil(item_count/192))

dataframe = pd.DataFrame()

for i in range(1, PAGE_COUNT+1): 
    LINK = BASE_LINK + "&_pgn=" + str(i)
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
    
    print("Scraped Products from Page {} Successfully".format(i))
    
    PER_ITEM = []

    for link in df['link']: 
        response = requests.get(link)
        page = BeautifulSoup(response.text, 'html.parser')
        
        try: 
            est_delivery = page.find('span', class_='vi-acc-del-range').text
        except: 
            pass
        
        try: 
            est_delivery = page.find('span', class_='vi-del-ship-txt').text
        except:
            pass
            
        try: 
            est_delivery = page.find('strong', class_='vi-acc-del-range').text
        except:
            pass
        
        try: 
            item_stock = page.find('span', class_='qtyTxt vi-bboxrev-dsplblk vi-qty-fixAlignment feedbackON vi-vpqp-feedback').find('span', id='qtySubTxt').text
        except:
            pass
        
        try:
            item_stock = page.find('span', id='qtySubTxt').text
        except:
            x = page.find_all('span', id="qtySubTxt")
            for y in x:
                z = i.find('span', class_="")
                item_stock = z.text
        
        labels = []
        values = []
        item_desc_df = pd.DataFrame()
        
        item_desc = page.find('div', class_='ux-layout-section ux-layout-section--features')
        
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

        for j in item_df[item_df['index'].isin(['MPN:'])]['values']: 
            mpn = j
            per_item_info = [link, item_stock, est_delivery, mpn]
            PER_ITEM.append(per_item_info)
            

    df1 = pd.DataFrame(data=PER_ITEM)
    df1.columns = ['link', 'ITEM_STOCK', 'ESTIMATED_DELIVERY', 'MPN']

    data = df.merge(df1, on='link', how='left')
    data['item_id'] = data['link'].str.split('?').str[0]
    data['item_id'] = data['item_id'].str.replace('https://www.ebay.co.uk/itm/', '', regex=True)

    dataframe = pd.concat([dataframe, data]).reset_index(drop=True)