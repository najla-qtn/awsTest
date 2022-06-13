#!/usr/bin/env python
# coding: utf-8

# In[4]:


import pandas as pd
import numpy as np
import json
import requests
import itertools
from pathlib import Path
from time import sleep
import datetime

base_url = 'https://comtrade.un.org/api/get?'

def mk_slice_points(reporter,partner,period,human_readable=False,product='all',frequency='A'):

    # (2) warn/ raise an error if appropriate

    if sum('all' in inpt for inpt in [reporter, partner, period]) > 1:
        raise ValueError("Only one of the parameters 'reporter', 'partner' and 'period' may use the special ALL value in a given API call.")

    if any(len(inpt) > 5 for inpt in [reporter, partner, period]) and human_readable:
        print("Using the option human_readable=True is not recommended in this case because several API calls are necessary.")
        print("When using the human_readable=True option, messages from the API cannot be received!")
        response = input("Press y if you want to continue anyways. ")
        if response != 'y':
            return None # exit function

    slice_points = [range(0, len(inpt), 5) for inpt in [reporter, partner, period]] +         [range(0, len(product), 20)]  
    
    return slice_points

def download_trade_data(filename, human_readable=False, verbose=True,
    period='recent', frequency='A', reporter='USA', partner='all', product='total', tradeflow='exports'):

    """
    Downloads records from the UN Comtrade database and saves them in a csv-file with the name "filename".
    If necessary, it calls the API several times.
    There are two modes:
    - human_readable = False (default): headings in output are not human-readable but error messages from the API are received and displayed
    - human_readable = True: headings in output are human-readable but we do not get messages from the API about potential problems (not recommended if several API calls are necessary)
    Additional option: verbose = False in order to suppress both messages from the API and messages like '100 records downloaded and saved in filename.csv' (True is default)
    Parameters:
    Using parameter values suggested in the API documentation should always work.
    For the parameters period, reporter, partner and tradeflow more intuitive options have been added.
     - period     [ps]   : depending on freq, either YYYY or YYYYMM (or 'YYYY-YYYY'/ 'YYYYMM-YYYYMM' or a list of those) or 'now' or 'recent' (= 5 most recent years/ months) or 'all'
     - frequency  [freq] : 'A' (= annual) or 'M' (= monthly)
     - reporter   [r]    : reporter code/ name (case-sensitive!) or list of reporter codes/ names or 'all' (see https://comtrade.un.org/data/cache/reporterAreas.json)
     - partner    [p]    : partner code/ name  (case-sensitive!) or list of partner codes/ names or 'all' (see https://comtrade.un.org/data/cache/partnerAreas.json)
     - product    [cc]   : commodity code valid in the selected classification (here: Harmonized System HS) or 'total' (= aggregated) or 'all' or 'HG2', 'HG4' or 'HG6' (= all 2-, 4- and 6-digit HS commodities)
     - tradeflow  [rg]   : 'import[s]' or 'export[s]'; see https://comtrade.un.org/data/cache/tradeRegimes.json for further, lower-level options
     Information copied from the API Documentation (https://comtrade.un.org/data/doc/api/):
     Usage limits
     Rate limit (guest): 1 request every second (per IP address or authenticated user).
     Usage limit (guest): 100 requests per hour (per IP address or authenticated user).
     Parameter combination limit: ps, r and p are limited to 5 codes each. Only one of the above codes may use the special ALL value in a given API call.
     Classification codes (cc) are limited to 20 items. ALL is always a valid classification code.
     If you hit a usage limit a 409 (conflict) error is returned along with a message specifying why the request was blocked and when requests may resume.
     Stability
     Notice: this API may be considered stable. However, new fields may be added in the future.
     While this API is still subject to change, changes that remove fields will be announced and a method of accessing legacy field formats will be made available during a transition period.
     New fields may be added to the CSV or JSON output formats without warning. Please write your code that accesses the API accordingly.
     """
    #no need to transfer since the id is passed not the namee
    
    reporter = reporter #transform_reporter(reporter)
    partner = partner #transform_partner(partner)
    period = transform_period(period, frequency)
    
    slice_points = mk_slice_points(reporter,partner,period,human_readable)
        
    # (3) download data by doing one or several API calls


    # since the parameters reporter, partner and period are limited to 5 inputs each and
    # product is limited to 20 inputs
    
    tradeflow = transform_tradeflow(tradeflow)
    
    dfs = []
    
    slices = itertools.product(*slice_points)
    r = 0 
    for i, j, k, m in slices:

        df = download_trade_data_base(human_readable=human_readable, verbose=verbose,
            period=period[k:k+5], reporter=reporter[i:i+5],
            partner=partner[j:j+5], product=product[m:m+20],
            tradeflow=tradeflow, frequency=frequency,filename=filename )
        r += 1

        if df is not None:
            dfs.append(df)

        sleep(1) # wait 1 second because of API rate limit

    # (4) save dataframe as csv file

    if len(dfs) > 0:
        df_all = pd.concat(dfs)
        filename = filename if len(filename.split('.')) >= 2 else filename + '.csv' # add '.csv' if necessary
        df_all.to_csv(filename)
        if verbose: print('{} records downloaded and saved as {}.'.format(len(df_all), filename))
            
    return (r)

def download_trade_data_base(human_readable=False, verbose=True,
    period='recent', frequency='A', reporter=842, partner='all', product='total', tradeflow=2,filename=None):

    """
    Downloads records from the UN Comtrade database and returns pandas dataframe using one API call.
    There are two modes:
    - human_readable = False (default): headings in output are not human-readable but error messages from the API are received and displayed
    - human_readable = True: headings in output are human-readable but we do not get messages from the API about potential problems
    Additional option: verbose = False in order to suppress messages from the API (True is default)
    Parameters of the API call:
    As documented in the API documentation.
    More intuitive options for the parameters period, reporter, partner and tradeflow are only available in the function 'download_trade_data'!
     - period     [ps]   : depending on freq, either YYYY or YYYYMM (or a list of those) or 'now' or 'recent' (= 5 most recent years/ months) or 'all'
     - frequency  [freq] : 'A' (= annual) or 'M' (= monthly)
     - reporter   [r]    : reporter code or list of reporter codes or 'all' (see https://comtrade.un.org/data/cache/reporterAreas.json)
     - partner    [p]    : partner code or list of partner codes or 'all' (see https://comtrade.un.org/data/cache/partnerAreas.json)
     - product    [cc]   : commodity code valid in the selected classification (here: Harmonized System HS) or 'total' (= aggregated) or 'all' or 'HG2', 'HG4' or 'HG6' (= all 2-, 4- and 6-digit HS commodities)
     - tradeflow  [rg]   : 1 (for imports) or 2 (for exports); see https://comtrade.un.org/data/cache/tradeRegimes.json for further options
    """

    fmt = 'csv' if human_readable else 'json'
    head = 'H' if human_readable else 'M'

    parameters = {
        'ps': period,
        'freq': frequency,
        'r': reporter,
        'p': partner,
        'cc': product,
        'rg': tradeflow,
        'px': 'HS',      # Harmonized System (as reported) as classification scheme
        'type': 'C',     # Commodities ('S' for Services)
        'fmt': fmt,      # format of the output
        'max': 100000,    # maximum number of rows -> what happens if number of rows is bigger?
                         # https://comtrade.un.org/data/dev/portal#subscription says it is 100 000
        'head': head     # human readable headings ('H') or machine readable headings ('M')
    }

    url = base_url + dict_to_string(parameters)

    if verbose: print(url)

    if human_readable:

        dataframe = pd.read_csv(url)

    else:

        json_dict = requests.get(url,timeout=120).json()

        n_records = json_dict['validation']['count']['value']
        message = json_dict['validation']['message']

        if not json_dict['dataset']:
            if verbose: print('Error: empty dataset \n Message: {}'.format(message))
            dataframe = None
            f = open(filename,"w+")
            f.close()

        else:
            if verbose and message: print('Message: {}'.format(message))
            dataframe = pd.DataFrame.from_dict(json_dict['dataset'])

    return dataframe




def transform_tradeflow(tradeflow):
    """
    replace tradeflow "import(s)" or "export(s)" by the corresponding numbers (1 / 2)
    """
    if isinstance(tradeflow, str):
        if 'export' in tradeflow.lower():
            tradeflow = 2
        elif 'import' in tradeflow.lower():
            tradeflow = 1
    return tradeflow


def transform_period(period, frequency):
    """
    detects 'YYYY-YYYY' or 'YYYYMM-YYYYMM' inputs and transforms them into lists of YYYY or YYYYMM that the API can understand
    the function does not check whether the other inputs for period are valid!
    period: depending on freq, either YYYY or YYYYMM (or 'YYYY-YYYY'/ 'YYYYMM-YYYYMM' or a list of those) or 'now' or 'recent' or 'all'
    frequency: 'A' or 'M'
    """

    period = [period] if not isinstance(period, list) else period

    period_new = []

    for p in period:

        if isinstance(p, str) and '-' in p:
            start, end = p.split('-')

            if frequency.lower() == 'a':
                y_start = int(start)
                y_end = int(end)
                for y in range(y_start, y_end + 1):
                    period_new.append(y)

            elif frequency.lower() == 'm':
                y_start, m_start = int(start[:4]), int(start[4:])
                y_end, m_end = int(end[:4]), int(end[4:])
                n = (m_end - m_start + 1) + 12 * (y_end - y_start)
                y, m = y_start, m_start
                for _ in range(n):
                    period_new.append('{}{:02d}'.format(y, m))
                    if m >= 1 and m < 12:
                        m +=1
                    elif m == 12:
                        m = 1
                        y += 1
                    else:
                        raise Exception("Shouldn't get here.")

            else:
                raise Exception("Frequency neither 'A'/'a' nor 'M'/'m'.")

        else:
            period_new.append(p)

    return period_new

def dict_item_to_string(key, value):
    """
    inputs: key-value pairs from a dictionary
    output: string 'key=value' or 'key=value1,value2' (if value is a list)
    examples: 'fmt', 'csv' => 'fmt=csv' or 'r', [124, 484] => 'r=124,484'
    """
    value_string = str(value) if not isinstance(value, list) else ','.join(map(str, value))
    return '='.join([key, value_string])


def dict_to_string(parameters):
    """
    input: dictionary of parameters
    output: string 'key1=value1&key2=value2&...'
    """
    return '&'.join(dict_item_to_string(key, value) for key, value in parameters.items())




# In[5]:


#Call details 1
trade_flows = ['import','Export']
reporters = ['682'] #USA 842
partners = ['660'] #Saudi 682
# periods = ['201001-201005','201006-201010','201011-201103','201104-201108','201109-201201','201202-201206',
#            '201207-201211','201212-201304','201305-201309','201310-201312']
periods = ['201607-201611']
# periods = ['201401-201405','201406-201410','201411-201503','201504-201508','201509-201601','201602-201606',
#            '201607-201611','201612-201704','201705-201709','201710-201712','201801-201805','201806-201810',
#            '201811-201812','201001-201005','201006-201010','201011-201103','201104-201108','201109-201201',
#            '201202-201206','201207-201211','201212-201304','201305-201309','201310-201312']
periods.reverse()


# In[6]:


#call funct1 => resulted HS code combined
i=0
r=0
rph = 95
wait_s = 60*60
for part in partners:
    for tf in trade_flows:
        for p in periods:
            for rep in reporters:
                print(i)
                file = '{}_{}_{}_{}.csv'.format(part,tf,p,rep)
                my_file = "/var/log/cadabra/"+file
                #/Users/NajlaAlqahtani/Downloads/cadabra/
                #my_file = Path(file)
                try:
                    f = open(my_file)
                    print("the file {} exists".format(file))
                except:
                    print('Requesting data for {}...'.format(file))
                    try:
                        reqs = download_trade_data(my_file, period=p, frequency='M', reporter=rep, 
                                                   partner=part, product='all', tradeflow=tf)
                    except Exception as e: 
                        print(e)
                        print("There was a problem downloading the data")
                        if "Expecting" in str(e):
                            wakingup_at = datetime.datetime.strftime(datetime.datetime.today() + datetime.timedelta(seconds = wait_s) , '%d/%m/%Y:%H:%M')
                            print("Sleeping for an hour, resuming at {}".format(wakingup_at))
                            sleep(wait_s)
                        sleep(1)
                    else:
                        r += reqs if reqs > 0 else 1
                        print("{} requests this session".format(r))
                        sleep(1)
            
                i += 1


# In[ ]:




