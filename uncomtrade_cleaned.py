{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "import pandas as pd\n",
    "import numpy as np\n",
    "import json\n",
    "import requests\n",
    "import os.path\n",
    "import itertools\n",
    "from pathlib import Path\n",
    "from time import sleep\n",
    "import datetime\n",
    "\n",
    "base_url = 'https://comtrade.un.org/api/get?'\n",
    "\n",
    "def mk_slice_points(reporter,partner,period,human_readable=False,product='all',frequency='A'):\n",
    "\n",
    "    # (2) warn/ raise an error if appropriate\n",
    "\n",
    "    if sum('all' in inpt for inpt in [reporter, partner, period]) > 1:\n",
    "        raise ValueError(\"Only one of the parameters 'reporter', 'partner' and 'period' may use the special ALL value in a given API call.\")\n",
    "\n",
    "    if any(len(inpt) > 5 for inpt in [reporter, partner, period]) and human_readable:\n",
    "        print(\"Using the option human_readable=True is not recommended in this case because several API calls are necessary.\")\n",
    "        print(\"When using the human_readable=True option, messages from the API cannot be received!\")\n",
    "        response = input(\"Press y if you want to continue anyways. \")\n",
    "        if response != 'y':\n",
    "            return None # exit function\n",
    "\n",
    "    slice_points = [range(0, len(inpt), 5) for inpt in [reporter, partner, period]] + \\\n",
    "        [range(0, len(product), 20)]  \n",
    "    \n",
    "    return slice_points\n",
    "\n",
    "def download_trade_data(filename, human_readable=False, verbose=True,\n",
    "    period='recent', frequency='A', reporter='USA', partner='all', product='total', tradeflow='exports'):\n",
    "\n",
    "    \"\"\"\n",
    "    Downloads records from the UN Comtrade database and saves them in a csv-file with the name \"filename\".\n",
    "    If necessary, it calls the API several times.\n",
    "    There are two modes:\n",
    "    - human_readable = False (default): headings in output are not human-readable but error messages from the API are received and displayed\n",
    "    - human_readable = True: headings in output are human-readable but we do not get messages from the API about potential problems (not recommended if several API calls are necessary)\n",
    "    Additional option: verbose = False in order to suppress both messages from the API and messages like '100 records downloaded and saved in filename.csv' (True is default)\n",
    "    Parameters:\n",
    "    Using parameter values suggested in the API documentation should always work.\n",
    "    For the parameters period, reporter, partner and tradeflow more intuitive options have been added.\n",
    "     - period     [ps]   : depending on freq, either YYYY or YYYYMM (or 'YYYY-YYYY'/ 'YYYYMM-YYYYMM' or a list of those) or 'now' or 'recent' (= 5 most recent years/ months) or 'all'\n",
    "     - frequency  [freq] : 'A' (= annual) or 'M' (= monthly)\n",
    "     - reporter   [r]    : reporter code/ name (case-sensitive!) or list of reporter codes/ names or 'all' (see https://comtrade.un.org/data/cache/reporterAreas.json)\n",
    "     - partner    [p]    : partner code/ name  (case-sensitive!) or list of partner codes/ names or 'all' (see https://comtrade.un.org/data/cache/partnerAreas.json)\n",
    "     - product    [cc]   : commodity code valid in the selected classification (here: Harmonized System HS) or 'total' (= aggregated) or 'all' or 'HG2', 'HG4' or 'HG6' (= all 2-, 4- and 6-digit HS commodities)\n",
    "     - tradeflow  [rg]   : 'import[s]' or 'export[s]'; see https://comtrade.un.org/data/cache/tradeRegimes.json for further, lower-level options\n",
    "     Information copied from the API Documentation (https://comtrade.un.org/data/doc/api/):\n",
    "     Usage limits\n",
    "     Rate limit (guest): 1 request every second (per IP address or authenticated user).\n",
    "     Usage limit (guest): 100 requests per hour (per IP address or authenticated user).\n",
    "     Parameter combination limit: ps, r and p are limited to 5 codes each. Only one of the above codes may use the special ALL value in a given API call.\n",
    "     Classification codes (cc) are limited to 20 items. ALL is always a valid classification code.\n",
    "     If you hit a usage limit a 409 (conflict) error is returned along with a message specifying why the request was blocked and when requests may resume.\n",
    "     Stability\n",
    "     Notice: this API may be considered stable. However, new fields may be added in the future.\n",
    "     While this API is still subject to change, changes that remove fields will be announced and a method of accessing legacy field formats will be made available during a transition period.\n",
    "     New fields may be added to the CSV or JSON output formats without warning. Please write your code that accesses the API accordingly.\n",
    "     \"\"\"\n",
    "    #no need to transfer since the id is passed not the namee\n",
    "    \n",
    "    reporter = reporter #transform_reporter(reporter)\n",
    "    partner = partner #transform_partner(partner)\n",
    "    period = transform_period(period, frequency)\n",
    "    \n",
    "    slice_points = mk_slice_points(reporter,partner,period,human_readable)\n",
    "        \n",
    "    # (3) download data by doing one or several API calls\n",
    "\n",
    "\n",
    "    # since the parameters reporter, partner and period are limited to 5 inputs each and\n",
    "    # product is limited to 20 inputs\n",
    "    \n",
    "    tradeflow = transform_tradeflow(tradeflow)\n",
    "    \n",
    "    dfs = []\n",
    "    \n",
    "    slices = itertools.product(*slice_points)\n",
    "    r = 0 \n",
    "    for i, j, k, m in slices:\n",
    "\n",
    "        df = download_trade_data_base(human_readable=human_readable, verbose=verbose,\n",
    "            period=period[k:k+5], reporter=reporter[i:i+5],\n",
    "            partner=partner[j:j+5], product=product[m:m+20],\n",
    "            tradeflow=tradeflow, frequency=frequency,filename=filename )\n",
    "        r += 1\n",
    "\n",
    "        if df is not None:\n",
    "            dfs.append(df)\n",
    "\n",
    "        sleep(1) # wait 1 second because of API rate limit\n",
    "\n",
    "    # (4) save dataframe as csv file\n",
    "\n",
    "    if len(dfs) > 0:\n",
    "        df_all = pd.concat(dfs)\n",
    "        filename = filename if len(filename.split('.')) >= 2 else filename + '.csv' # add '.csv' if necessary\n",
    "        df_all.to_csv(filename)\n",
    "        if verbose: print('{} records downloaded and saved as {}.'.format(len(df_all), filename))\n",
    "            \n",
    "    return (r)\n",
    "\n",
    "def download_trade_data_base(human_readable=False, verbose=True,\n",
    "    period='recent', frequency='A', reporter=842, partner='all', product='total', tradeflow=2,filename=None):\n",
    "\n",
    "    \"\"\"\n",
    "    Downloads records from the UN Comtrade database and returns pandas dataframe using one API call.\n",
    "    There are two modes:\n",
    "    - human_readable = False (default): headings in output are not human-readable but error messages from the API are received and displayed\n",
    "    - human_readable = True: headings in output are human-readable but we do not get messages from the API about potential problems\n",
    "    Additional option: verbose = False in order to suppress messages from the API (True is default)\n",
    "    Parameters of the API call:\n",
    "    As documented in the API documentation.\n",
    "    More intuitive options for the parameters period, reporter, partner and tradeflow are only available in the function 'download_trade_data'!\n",
    "     - period     [ps]   : depending on freq, either YYYY or YYYYMM (or a list of those) or 'now' or 'recent' (= 5 most recent years/ months) or 'all'\n",
    "     - frequency  [freq] : 'A' (= annual) or 'M' (= monthly)\n",
    "     - reporter   [r]    : reporter code or list of reporter codes or 'all' (see https://comtrade.un.org/data/cache/reporterAreas.json)\n",
    "     - partner    [p]    : partner code or list of partner codes or 'all' (see https://comtrade.un.org/data/cache/partnerAreas.json)\n",
    "     - product    [cc]   : commodity code valid in the selected classification (here: Harmonized System HS) or 'total' (= aggregated) or 'all' or 'HG2', 'HG4' or 'HG6' (= all 2-, 4- and 6-digit HS commodities)\n",
    "     - tradeflow  [rg]   : 1 (for imports) or 2 (for exports); see https://comtrade.un.org/data/cache/tradeRegimes.json for further options\n",
    "    \"\"\"\n",
    "\n",
    "    fmt = 'csv' if human_readable else 'json'\n",
    "    head = 'H' if human_readable else 'M'\n",
    "\n",
    "    parameters = {\n",
    "        'ps': period,\n",
    "        'freq': frequency,\n",
    "        'r': reporter,\n",
    "        'p': partner,\n",
    "        'cc': product,\n",
    "        'rg': tradeflow,\n",
    "        'px': 'HS',      # Harmonized System (as reported) as classification scheme\n",
    "        'type': 'C',     # Commodities ('S' for Services)\n",
    "        'fmt': fmt,      # format of the output\n",
    "        'max': 100000,    # maximum number of rows -> what happens if number of rows is bigger?\n",
    "                         # https://comtrade.un.org/data/dev/portal#subscription says it is 100 000\n",
    "        'head': head     # human readable headings ('H') or machine readable headings ('M')\n",
    "    }\n",
    "\n",
    "    url = base_url + dict_to_string(parameters)\n",
    "\n",
    "    if verbose: print(url)\n",
    "\n",
    "    if human_readable:\n",
    "\n",
    "        dataframe = pd.read_csv(url)\n",
    "\n",
    "    else:\n",
    "\n",
    "        json_dict = requests.get(url,timeout=120).json()\n",
    "\n",
    "        n_records = json_dict['validation']['count']['value']\n",
    "        message = json_dict['validation']['message']\n",
    "\n",
    "        if not json_dict['dataset']:\n",
    "            if verbose: print('Error: empty dataset \\n Message: {}'.format(message))\n",
    "            dataframe = None\n",
    "            f = open(filename,\"w+\")\n",
    "            f.close()\n",
    "\n",
    "        else:\n",
    "            if verbose and message: print('Message: {}'.format(message))\n",
    "            dataframe = pd.DataFrame.from_dict(json_dict['dataset'])\n",
    "\n",
    "    return dataframe\n",
    "\n",
    "\n",
    "\n",
    "\n",
    "def transform_tradeflow(tradeflow):\n",
    "    \"\"\"\n",
    "    replace tradeflow \"import(s)\" or \"export(s)\" by the corresponding numbers (1 / 2)\n",
    "    \"\"\"\n",
    "    if isinstance(tradeflow, str):\n",
    "        if 'export' in tradeflow.lower():\n",
    "            tradeflow = 2\n",
    "        elif 'import' in tradeflow.lower():\n",
    "            tradeflow = 1\n",
    "    return tradeflow\n",
    "\n",
    "\n",
    "def transform_period(period, frequency):\n",
    "    \"\"\"\n",
    "    detects 'YYYY-YYYY' or 'YYYYMM-YYYYMM' inputs and transforms them into lists of YYYY or YYYYMM that the API can understand\n",
    "    the function does not check whether the other inputs for period are valid!\n",
    "    period: depending on freq, either YYYY or YYYYMM (or 'YYYY-YYYY'/ 'YYYYMM-YYYYMM' or a list of those) or 'now' or 'recent' or 'all'\n",
    "    frequency: 'A' or 'M'\n",
    "    \"\"\"\n",
    "\n",
    "    period = [period] if not isinstance(period, list) else period\n",
    "\n",
    "    period_new = []\n",
    "\n",
    "    for p in period:\n",
    "\n",
    "        if isinstance(p, str) and '-' in p:\n",
    "            start, end = p.split('-')\n",
    "\n",
    "            if frequency.lower() == 'a':\n",
    "                y_start = int(start)\n",
    "                y_end = int(end)\n",
    "                for y in range(y_start, y_end + 1):\n",
    "                    period_new.append(y)\n",
    "\n",
    "            elif frequency.lower() == 'm':\n",
    "                y_start, m_start = int(start[:4]), int(start[4:])\n",
    "                y_end, m_end = int(end[:4]), int(end[4:])\n",
    "                n = (m_end - m_start + 1) + 12 * (y_end - y_start)\n",
    "                y, m = y_start, m_start\n",
    "                for _ in range(n):\n",
    "                    period_new.append('{}{:02d}'.format(y, m))\n",
    "                    if m >= 1 and m < 12:\n",
    "                        m +=1\n",
    "                    elif m == 12:\n",
    "                        m = 1\n",
    "                        y += 1\n",
    "                    else:\n",
    "                        raise Exception(\"Shouldn't get here.\")\n",
    "\n",
    "            else:\n",
    "                raise Exception(\"Frequency neither 'A'/'a' nor 'M'/'m'.\")\n",
    "\n",
    "        else:\n",
    "            period_new.append(p)\n",
    "\n",
    "    return period_new\n",
    "\n",
    "def dict_item_to_string(key, value):\n",
    "    \"\"\"\n",
    "    inputs: key-value pairs from a dictionary\n",
    "    output: string 'key=value' or 'key=value1,value2' (if value is a list)\n",
    "    examples: 'fmt', 'csv' => 'fmt=csv' or 'r', [124, 484] => 'r=124,484'\n",
    "    \"\"\"\n",
    "    value_string = str(value) if not isinstance(value, list) else ','.join(map(str, value))\n",
    "    return '='.join([key, value_string])\n",
    "\n",
    "\n",
    "def dict_to_string(parameters):\n",
    "    \"\"\"\n",
    "    input: dictionary of parameters\n",
    "    output: string 'key1=value1&key2=value2&...'\n",
    "    \"\"\"\n",
    "    return '&'.join(dict_item_to_string(key, value) for key, value in parameters.items())\n",
    "\n",
    "\n",
    "\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "#Call details 1\n",
    "trade_flows = ['import','Export']\n",
    "reporters = ['682'] #USA 842\n",
    "partners = ['660'] #Saudi 682\n",
    "# periods = ['201001-201005','201006-201010','201011-201103','201104-201108','201109-201201','201202-201206',\n",
    "#            '201207-201211','201212-201304','201305-201309','201310-201312']\n",
    "periods = ['201607-201611']\n",
    "# periods = ['201401-201405','201406-201410','201411-201503','201504-201508','201509-201601','201602-201606',\n",
    "#            '201607-201611','201612-201704','201705-201709','201710-201712','201801-201805','201806-201810',\n",
    "#            '201811-201812','201001-201005','201006-201010','201011-201103','201104-201108','201109-201201',\n",
    "#            '201202-201206','201207-201211','201212-201304','201305-201309','201310-201312']\n",
    "periods.reverse()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "#call funct1 => resulted HS code combined\n",
    "i=0\n",
    "r=0\n",
    "rph = 95\n",
    "wait_s = 60*60\n",
    "for part in partners:\n",
    "    for tf in trade_flows:\n",
    "        for p in periods:\n",
    "            for rep in reporters:\n",
    "                print(i)\n",
    "                file = '{}_{}_{}_{}.csv'.format(part,tf,p,rep)\n",
    "                my_file = Path(file)\n",
    "                if not my_file.is_file():\n",
    "                    print('Requesting data for {}...'.format(file))\n",
    "                    try:\n",
    "                        reqs = download_trade_data(file, period=p, frequency='M', reporter=rep, \n",
    "                                                   partner=part, product='all', tradeflow=tf)\n",
    "                    except Exception as e: \n",
    "                        print(e)\n",
    "                        print(\"There was a problem downloading the data\")\n",
    "                        if \"Expecting\" in str(e):\n",
    "                            wakingup_at = datetime.datetime.strftime(datetime.datetime.today() + datetime.timedelta(seconds = wait_s) , '%d/%m/%Y:%H:%M')\n",
    "                            print(\"Sleeping for an hour, resuming at {}\".format(wakingup_at))\n",
    "                            sleep(wait_s)\n",
    "                        sleep(1)\n",
    "                    else:\n",
    "                        r += reqs if reqs > 0 else 1\n",
    "                        print(\"{} requests this session\".format(r))\n",
    "                        sleep(1)\n",
    "                else:\n",
    "                    print(\"the file {} exists\".format(file))\n",
    "                i += 1\n"
   ]
  },
  {
   "cell_type": "raw",
   "metadata": {},
   "source": [
    "NIC: Sleeping for an hour, resuming at 06/12/2018:18:55\n",
    "Phone: Sleeping for an hour, resuming at 06/12/2018:18:16\n",
    "dongle: Sleeping for an hour, resuming at 06/12/2018:18:36"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.7.4"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 2
}