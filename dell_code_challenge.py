import requests
from requests.adapters import HTTPAdapter, Retry
import pandas as pd
import sqlalchemy
import os
from io import StringIO
import json
import geo

USER_NAME = 'postgres'
PASSWORD = 'heslo123'
DB_NAME = 'postgres'
DB_SCHEMA = 'public'
FILE_LOCATION = '/Users/marosurminsky/Downloads/'
FILE_NAME = 'countries of the world.csv'


def get_api_data(url):
    
    #configuring session
    s = requests.Session()
    retries = Retry(total=5,
                    backoff_factor=0.1,
                    status_forcelist=[ 500, 502, 503, 504 ])
    s.mount('https://', HTTPAdapter(max_retries=retries))
    try:
        resp = s.get(url) 
    except Exception as e:
        print('call to {} failed with an exception{}'.format(url, e))
    return resp


def loading_data():

    # Data source 1
    # the source does not contain data required in exercise 2 & 4, due to weekly granularity instead of daily
    engine = sqlalchemy.create_engine('postgresql://{}:{}@localhost:5432/{}'.format(USER_NAME, PASSWORD, DB_NAME),connect_args={'options': '-csearch_path={}'.format(DB_SCHEMA)})
    ecdc_weekly_resp = get_api_data('https://opendata.ecdc.europa.eu/covid19/nationalcasedeath/json')
    if ecdc_weekly_resp.ok:
        try:
            df_covid19_weekly = pd.read_json(StringIO(ecdc_weekly_resp.text))
            df_covid19_weekly.columns = df_covid19_weekly.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '').str.replace('.', '')
            df_covid19_weekly.to_sql('ecdc_covid19_weekly', engine, if_exists='replace')
        except Exception as e:
            print('loading df_covid19_weekly to db failed with an exception {}'.format(e))
    else:
        print('endpoint ecdc weekly returned failed status code: {}'.format(ecdc_weekly_resp.status_code))
    
    # added daily data to fullfil required exercises
    ecdc_daily_resp = get_api_data('https://opendata.ecdc.europa.eu/covid19/nationalcasedeath_eueea_daily_ei/json')
    if ecdc_daily_resp.ok:
        try:
            covid19_daily_dict = json.loads(ecdc_daily_resp.text)['records'] 
            df_covid19_daily = pd.read_json(StringIO(json.dumps(covid19_daily_dict)))
            df_covid19_daily.columns = df_covid19_daily.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '').str.replace('.', '')
            df_covid19_daily['daterep'] = pd.to_datetime(df_covid19_daily['daterep'], errors='raise', dayfirst=True)
            df_covid19_daily.to_sql('ecdc_covid19_daily', engine, if_exists='replace')
        except Exception as e:
            print('loading df_covid19_daily to db failed with an exception {}'.format(e))
    else:
        print('endpoint ecdc daily returned failed status code: {}'.format(ecdc_daily_resp.status_code))


    # Data source 2
        
    csv_file = os.path.join(FILE_LOCATION, FILE_NAME)

    if (os.path.isfile(csv_file)):
        try:
            df_countries = pd.read_csv(csv_file, skipinitialspace=True)
        except Exception as e:
            print('loading csv file failed with an exception {}'.format(e))
        else:
            try:
                df_countries.columns = df_countries.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace('%', 'percent').str.replace('/', '_slash_').str.replace('.', '').str.replace(')', '')
                for col in df_countries.select_dtypes(include=['object']).columns:
                    df_countries[col] = df_countries[col].str.strip()
                df_countries.to_sql('countries', engine, if_exists='replace')
            except Exception as e:
                print('loading df_countries to db failed with an exception {}'.format(e))
    print('end of loading files to db')

def create_pipeline():

    """ Pipeline extracts full table from postgres and performs anti-join with the endpoint response
        I choose this logic because this way are all missing records filled to db including records from past
        in case there is an outage, not only records since latest date
    """
    engine = sqlalchemy.create_engine('postgresql://{}:{}@localhost:5432/{}'.format(USER_NAME, PASSWORD, DB_NAME),connect_args={'options': '-csearch_path={}'.format(DB_SCHEMA)})
    query_s = 'select * from ecdc_covid19_daily;'
    with engine.connect() as conn, conn.begin():
        df_ecdc_covid19_daily = pd.read_sql_query(query_s, conn)
    
    ecdc_daily_resp = get_api_data('https://opendata.ecdc.europa.eu/covid19/nationalcasedeath_eueea_daily_ei/json')
    if ecdc_daily_resp.ok:
        covid19_daily_dict = json.loads(ecdc_daily_resp.text)['records'] 
        df_covid19_daily = pd.read_json(StringIO(json.dumps(covid19_daily_dict)))
        df_covid19_daily.columns = df_covid19_daily.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '').str.replace('.', '')
        df_covid19_daily['daterep'] = pd.to_datetime(df_covid19_daily['daterep'], errors='raise', dayfirst=True)
        df_outer = df_covid19_daily.merge(df_ecdc_covid19_daily, how='outer', indicator=True)
        df_anti_joined = df_outer[(df_outer._merge=='left_only')].drop('_merge', axis=1).drop('index', axis='columns')
        try:
            df_anti_joined.to_sql('ecdc_covid19_daily', engine, if_exists='append')
        except Exception as e:
            print('loading df_covid19_daily to db failed with an exception {}'.format(e))
    else:
        print('endpoint ecdc daily returned failed status code: {}'.format(ecdc_daily_resp.status_code))
    print('end of pipeline')


def enrich():
    """added who comprehensive dataset containing whole world"""
    who_daily = get_api_data('https://covid19.who.int/WHO-COVID-19-global-data.csv')
    engine = sqlalchemy.create_engine('postgresql://{}:{}@localhost:5432/{}'.format(USER_NAME, PASSWORD, DB_NAME),connect_args={'options': '-csearch_path={}'.format(DB_SCHEMA)})
    if who_daily.ok:
        try:
            df_who_daily = pd.read_csv(StringIO(who_daily.content.decode('utf-8')))
            df_who_daily.columns = df_who_daily.columns.str.strip().str.lower()
            df_who_daily['date_reported'] = pd.to_datetime(df_who_daily['date_reported'], errors='raise', yearfirst=True)
            df_who_daily.to_sql('who_covid19_daily', engine, if_exists='replace')
        except Exception as e:
            print('loading df_who_daily to db failed with an exception {}'.format(e))
    else:
        print('endpoint who daily returned failed status code: {}'.format(df_who_daily.status_code))


def visualize():
    engine = sqlalchemy.create_engine('postgresql://{}:{}@localhost:5432/{}'.format(USER_NAME, PASSWORD, DB_NAME),connect_args={'options': '-csearch_path={}'.format(DB_SCHEMA)})
    #data = 'select distinct country from who_covid19_daily;'
    data = 'select distinct countriesandterritories from ecdc_covid19_daily;'
    with engine.connect() as conn, conn.begin():
        df_data = pd.read_sql_query(data, conn)
    #df_data_countries = df_data['country'].values.tolist()
    ecdc_countries = df_data['countriesandterritories'].values.tolist()
    #geo.render_countries(df_data_countries, 'who')
    geo.render_countries(ecdc_countries, 'ecdc')


    
if __name__ == "__main__":
    # EXERCISE 1:
    loading_data()
    
    # EXERCISE 2:
    create_pipeline()
    
    # EXERCISE 5:
    enrich()

    # VISUALIZE:
    #visualize()