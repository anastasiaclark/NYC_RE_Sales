# coding: utf-8
import pandas as pd
import numpy as np
import os, re, time
import sqlite3 as lite
from nyc_geoclient import Geoclient

# read-in NYC Geoclient API token
token=open('NYC_Geoclient_token.txt')
g=Geoclient(token.readline().strip('\n'), token.readline().strip('\n'))

def clean_strings(x):
    try:
        return str(x).strip()
    except ValueError:
        return np.nan
    
def parse_address(address):
    separators=['Apt','APT','#']
    # gets rid of the apartments in the address
    for separator in separators:
        if address.find(separator)!=-1:
            no_apt=address.split(separator,1)[0]
        else:
            no_apt=address
        #apartments can also be indicated by comma followed by number with optional letter (ex: , 503C)
        street=re.split(r'(,\s\d+$|,\s\d+\w{1}$)', no_apt)
        street=street[0] # grab what came before the apartment
        
        #separate address into street number and street name.
    split=re.split(r'(^\d+\s|^\d+-\d+\s|^\d+\w{1}\s|^\d+-\d+\w{1}\s)', street)
    if split[0]=='':            
        split.remove(split[0])
    # strip trailing spaces
    return [_.strip() for _ in split]
        
def AddressMatch(str_num,str_name,borough): ## function to geocode street addresses
    geocode=g.address(str_num, str_name, borough)
    message=geocode.get('message')
    latitude=geocode.get('latitude')
    longtitude=geocode.get('longitude')
    GeocodeResult='Address Match'
    return [longtitude, latitude, GeocodeResult, message]

def BlockMatch(borough, block,lot): ## function to geocode blocks and lots
    geocode=g.bbl(borough, block,lot)
    message=geocode.get('message')
    latitude=geocode.get('latitudeInternalLabel')
    longtitude=geocode.get('longitudeInternalLabel')
    GeocodeResult='Block Match'
    return [longtitude, latitude, GeocodeResult, message]

def Geocode(df):
    start=int(input('''From what line should I start? 
    Type in numercial value; type 0 (zero) for the first iteration \n'''))
    counter=0
    for index, row in df.iterrows():
        # geocode only from starting passed as input
        if index==0 or index > start:    
            counter=counter+1
            if counter % 5000==0:
                print '{} records have been geocoded'.format(counter)
            if counter%50==0:
                # will pause for 1 second after 50 geocoded records
                time.sleep(1)
            try:
                # do Address match first
                result=AddressMatch(row['street_number'], row['street_name'], int(row['borough']) )

                # if longtitude is None-->invalid addres, then try BlockMatch function
                if result[0] is None:
                    result=BlockMatch(int(row['borough']), int(row['block']), int(row['lot']) )

                    # if BlockMatch didn't return longtitude, mark the record as Unmatched in place of result
                    if result[0] is None:
                        result[2]='Unmatched'

                # for the db, remove last two items (parsed addrres) from the dataframe row and add geocoded results
                db_row=list(row[0:-2])+result

                # writing geocoded record into a database
                cur=con.cursor()
                table_name='yr{}'.format(year)
                cur.execute('''CREATE TABLE IF NOT EXISTS %s (sale_id INTEGER PRIMARY KEY, bbl_id INTEGER, 
                year TEXT, borough INTEGER,nbhd TEXT, bldg_ctgy TEXT,
                tax_cls_p TEXT, block TEXT,lot TEXT,easmnt TEXT, bldg_cls_p TEXT,address TEXT,
                apt TEXT, zip TEXT, res_unit INTEGER,com_unit INTEGER, tot_unit INTEGER, land_sqft INTEGER,
                tot_sqft INTEGER, yr_built INTEGER, tax_cls_s TEXT, bldg_cls_s TEXT,sale_date TEXT, price INTEGER,
                usable TEXT, long REAL, lat REAL, georesult TEXT, message TEXT)'''% table_name)

                qMark='?,'*28
                placeholder=qMark[:-1]       
                cur.execute('''INSERT INTO %s(bbl_id, year, borough, nbhd, bldg_ctgy, tax_cls_p, block,lot,
                easmnt, bldg_cls_p, address, apt, zip, res_unit, com_unit, tot_unit, land_sqft, tot_sqft,
                yr_built, tax_cls_s, bldg_cls_s, sale_date, price, usable, long, lat, 
                georesult, message) VALUES (%s)''' % (table_name,placeholder), db_row)        
                con.commit()    

            except Exception as e:
                print e
                print('An error has occurred. File stopped at index {}'.format(index))
                break
    con.close()    
    print 'Done'


data_path='/Users/anastasiaclark/NYC_RE_Sales'
# turn year into user input after testing
year=int(input('Type-in the folder name: Ex:2017'))

# read each borough sales and put it in a list temporarily
df_list=[]
data_folder=os.path.join(data_path,year)
boro_sales=[table for table in os.listdir(data_folder) if not 'citywide_sales' in table ]
for boro_table in boro_sales:
    df=pd.read_excel(os.path.join(data_folder,boro_table),skiprows=[0,1,2,3], parse_dates=True)
    df_list.append(df)

# get all boroughs sales into a single table
sales=pd.concat(df_list, ignore_index=True)
# clean-up column names
sales.columns=[c.strip() for c in sales.columns]

# give shorter names
sales.rename(columns={'BOROUGH': 'borough','NEIGHBORHOOD':'nbhd','BUILDING CLASS CATEGORY':'bldg_ctgy',
                   'TAX CLASS AT PRESENT':'tax_cls_p','BLOCK':'block','LOT':'lot',
                      'EASE-MENT':'easmnt','BUILDING CLASS AT PRESENT':'bldg_cls_p','ADDRESS':'address',
                   'APARTMENT NUMBER':'apt','ZIP CODE':'zip','RESIDENTIAL UNITS':'res_unit',
                   'COMMERCIAL UNITS':'com_unit','TOTAL UNITS':'tot_unit',
                   'LAND SQUARE FEET':'land_sqft','GROSS SQUARE FEET':'tot_sqft',
                   'YEAR BUILT':'yr_built','TAX CLASS AT TIME OF SALE':'tax_cls_s',
                   'BUILDING CLASS AT TIME OF SALE':'bldg_cls_s',
                      'SALE PRICE':'price','SALE DATE':'sale_date'}, 
          inplace=True)

# in 2017 DOF changed the column names, assuming that column 'BUILDING CLASS AS OF FINAL ROLL 17/18' will have different
# ending, locate them using the regex and rename them
sales.rename(columns={sales.filter(regex='BUILDING CLASS AS OF FINAL ROLL*').columns[0]: 'bldg_cls_p', 
                      sales.filter(regex='TAX CLASS AS OF FINAL ROLL*').columns[0]: 'tax_cls_p'}, inplace=True)

# strip trailing spaces from text columns
text_cols=[c for c in sales.columns if sales[c].dtype=='object']
for c in text_cols:
    sales[c]=sales[c].apply(lambda x: clean_strings(x))

# add some columns    
sales['bbl_id']=sales['borough'].astype(str)+sales['block'].astype(str)+sales['lot'].astype(str)
sales['usable']=np.where(sales['price']>10,'True','False')
sales['year']='{}'.format(year)
# SQLite doesn't support pandas datetime format, change dates to text
sales['sale_date']=sales['sale_date'].astype(str)


# re-arrange the order of the columns to be same as in the past
cols_order=['bbl_id', 'year', 'borough', 'nbhd', 'bldg_ctgy', 'tax_cls_p', 'block', 'lot',
            'easmnt', 'bldg_cls_p', 'address', 'apt', 'zip', 'res_unit', 'com_unit', 
            'tot_unit', 'land_sqft', 'tot_sqft', 'yr_built', 'tax_cls_s', 'bldg_cls_s', 
            'sale_date', 'price', 'usable']

sales=sales.loc[:, cols_order]


# separate address into individual parts (needed for geocoding parameters)
sales[['street_number', 'street_name']]=sales.apply(lambda row: pd.Series(parse_address(row['address'])), axis=1)


# this line runs the Geocode function
con=lite.connect('RE_Sales_beta.sqlite')
Geocode(sales)

