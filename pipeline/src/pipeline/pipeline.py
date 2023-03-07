import os.path
import feedparser
from google.cloud import translate_v2 as google_translate
from googleapiclient.discovery import build
from google.oauth2 import service_account
# from transformers import pipeline
import ast
import numpy as np
import tweepy
import pandas as pd
from tqdm import tqdm
import re
from time import sleep
import json
from dotenv import load_dotenv
credentials_path = '../credentials'
if os.path.exists(f"{credentials_path}/.env"):
    load_dotenv(dotenv_path=f"{credentials_path}/.env")
import traceback
import sys
import logging
from bs4 import BeautifulSoup
import requests

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s : %(levelname)s : %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("requests_oauthlib").setLevel(logging.WARNING)


def get_url_from_entities(entities):
    try:
        return entities['urls'][0]['expanded_url']
    except:
        return np.nan


def get_url_from_tweet(row):
    return f"https://twitter.com/{row['screen_name']}/status/{row['id']}"


def format_df(df_tweets):
    df_tweets['user'] = df_tweets['user'].astype(str).apply(ast.literal_eval)
    df_tweets['source'] = df_tweets['user'].apply(lambda x: x['name'])
    df_tweets['screen_name'] = df_tweets['user'].apply(lambda x: x['screen_name'])
    df_tweets['entities'] = df_tweets['entities'].astype(str).apply(ast.literal_eval)
    df_tweets['url'] = df_tweets['entities'].apply(get_url_from_entities)
    df_tweets['twitter_url'] = df_tweets.apply(get_url_from_tweet, axis=1)
    df_tweets['url'] = df_tweets['url'].fillna(df_tweets['twitter_url'])
    df_tweets['created_at'] = pd.to_datetime(df_tweets['created_at'])
    df_tweets['created_at'] = df_tweets['created_at'].dt.tz_localize(None)
    df_tweets = df_tweets[['created_at', 'id', 'full_text', 'source', 'geo', 'coordinates', 'place', 'retweet_count',
                           'favorite_count', 'possibly_sensitive', 'lang', 'url']]
    return df_tweets


def main():

    import datetime
    utc_timestamp = (
        datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    )

    try:
        # initialize google sheets api
        SCOPES = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/cloud-translation'
        ]
        spreadsheet_id = '1p8zMlaXlC-3BpPbl5Yb61u6VZRUIxD1Gc2yo7PJ9ScY'
        spreadsheet_range = 'Articles!A:G'
        service_account_info = json.load(open(f"{credentials_path}/google-service-account-template.json"))
        service_account_info['private_key_id'] = os.environ['PRIVATE_KEY_ID']
        service_account_info['private_key'] = os.environ['PRIVATE_KEY'].replace(r'\n', '\n')
        service_account_info['client_id'] = os.environ['CLIENT_ID']
        creds = service_account.Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=creds)

        # initialize the translator
        translator = google_translate.Client(credentials=creds)
        # translator = pipeline("translation_tr_to_en", model=f"Helsinki-NLP/opus-mt-tr-en")

        # get data already in the spreadsheet
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id,
                                                     range=spreadsheet_range).execute()
        values = result.get('values', [])
        df_old_values = pd.DataFrame.from_records(values[1:], columns=values[0])
        df_old_values['datetime'] = pd.to_datetime(
            df_old_values['Date'].astype(str) + ' ' + df_old_values['Time'].astype(str),
            format="%d/%m/%Y %H:%M"
        )

        # data sources
        sources = {
            'Alahednews': 'https://www.alahednews.com.lb/rss/',
            'Enab Baladi': 'https://www.enabbaladi.net/feed',
            'Daily Sabah': 'https://www.dailysabah.com/rssFeed/home-page',
            'Kurdpress': 'https://kurdpress.com/rss.php?lang=fa&cat=10',
            'Al Jazeera': 'https://www.aljazeera.com/xml/rss/all.xml',
            'Al Arabiya': 'https://www.alarabiya.net/feed/rss2/ar.xml',
            'Middle East Monitor': 'https://www.middleeastmonitor.com/feed/'
        }

        english_query = ["Syrian Arab Red Crescent", "Syrian Red Crescent", "Khaled Hboubati", "Khaled Erksoussi",
                         "Hossam Elsharkawi", "Mey Al Sayegh", "Idlib", "Idleb", "Safe access", "sanctions",
                         "cross-border aid", "crossline operations", "cholera", "north-west Syria", "northwest Syria",
                         "quake", "earthquake"]
        arabic_query = ["إدلب", "العبور الآمن", " عقوبات", "مساعدات عبر الحدود", "العمليات عبر الحدود", "الكوليرا",
                        "شمالي غربي سوريا", "إدلب", "العبور الآمن", " عقوبات", "مساعدات عبر الحدود",
                        "العمليات عبر الحدود", "الكوليرا", "هزة أرضية", "شمالي غربي سوريا"]
        keywords = english_query + arabic_query

        entries = []

        for source_name, source_url in sources.items():
            logging.info(f'Start source {source_name}')
            # get news feed
            NewsFeed = feedparser.parse(source_url)

            for entry in NewsFeed.entries:
                if any(x not in entry.keys() for x in ['id', 'published', 'link', 'title']):
                    continue

                datetime_entry = pd.to_datetime(entry['published'])

                if not df_old_values.empty:
                    # skip if link already present in google sheet
                    if entry['link'] in df_old_values['Link'].unique():
                        continue
                    # skip if older than latest news
                    if datetime_entry < df_old_values[df_old_values['Source'] == source_name]['datetime'].max().tz_localize(
                            'UTC+03:00'):
                        print(f"{datetime_entry} is older than {df_old_values['datetime'].max()}, skipping")
                        continue
                    else:
                        print(f"{datetime_entry} is newer than {df_old_values['datetime'].max()}, saving")

                title = re.sub(r"<(.*)>", "", entry['title'])  # clean title (without HTML leftovers)
                title_en = title  # translator.translate(title, target_language="en")["translatedText"]  # translate title to english

                res = requests.get(entry['id'])
                if res.status_code == 200:
                    soup = BeautifulSoup(res.content, 'html.parser')
                    text = [p.get_text() for p in soup.find_all('p')]
                    content = ' '.join(text.copy())
                    text_en = text  # [translator.translate(x, target_language="en")["translatedText"] for x in text]
                    content_en = ' '.join(text_en.copy())
                elif 'summary' in entry.keys():
                    content = re.sub(r"<(.*)>", "", entry['summary'])  # clean summary (without HTML leftovers)
                    content_en = content  # translator.translate(content, target_language="en")["translatedText"]
                else:
                    content = title
                    content_en = title_en

                # filter by location
                if not ('syria' in title_en.lower() or 'syria' in content_en.lower() or
                        'سوريا' in title.lower() or 'سوريا' in content.lower()):
                    logging.info('This entry is not about Syria:')
                    logging.info(f"{title_en}")
                    logging.info(f"{content_en}")
                    logging.info('---------------------------------------')
                    continue

                # filter by keyword
                if not any(keyword.lower() in title_en.lower() or keyword.lower() in content_en.lower() for keyword in
                           keywords):
                    logging.info('This entry is not relevant:')
                    logging.info(f"{title_en}")
                    logging.info(f"{content_en}")
                    logging.info('---------------------------------------')
                    continue

                # create simple entry
                entry_simple = {
                    'Date': datetime_entry.strftime("%d/%m/%Y"),
                    'Time': datetime_entry.strftime("%H:%M"),
                    # 'Title (en)': title_en,
                    'Title': title,
                    # 'Content (en)': content_en,
                    'Content': content,
                    'Source': source_name,
                    'Source+datetime': f'{source_name}, {datetime_entry.strftime("%d/%m/%Y")} {datetime_entry.strftime("%H:%M")}',
                    'Link': entry['link'],
                    'datetime': datetime_entry
                }
                entries.append(entry_simple)

        entries_sorted = sorted(entries, key=lambda d: d['datetime'])  # sort entries by date (from oldest to newest)

        # add entries to google sheet
        logging.info('updating Google sheet')
        for entry in tqdm(entries_sorted):
            # add new row to google sheet
            body = {'values': [list(entry.values())[:-1]]}
            result = service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id, range=spreadsheet_range,
                valueInputOption="USER_ENTERED", insertDataOption="INSERT_ROWS", body=body).execute()
            sleep(1)

        # Twitter
        twitter_sources = {
            'SANA Syria': 'SANAEnOfficial',
            'Alwatan Syria': 'AlwatanSy',
            'HashtagSyria': 'presshashtag',
            'Almasdar Online': 'AlmasdaronlineE',
            'Alghad': 'AlghadNews',
            'Shaam': 'ShaamNetwork',
            'Syrian Observatory for Human Rights': 'syriahr',
            'Baladi News': 'baladinetwork',
            'North Press Agency': 'NPA_Arabic',
            'Sky News Arabia': 'skynewsarabia',
            'Al Maydeen': 'Almayadeennews',
            'Monte Carlo Doualiya': 'MC_Doualiya',
            'BBC Arabic': 'BBCArabic'
        }

        spreadsheet_id = '1p8zMlaXlC-3BpPbl5Yb61u6VZRUIxD1Gc2yo7PJ9ScY'
        spreadsheet_range = 'Tweets!A:L'
        # get data already in the spreadsheet
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id,
                                                     range=spreadsheet_range).execute()
        values = result.get('values', [])
        df_old_values = pd.DataFrame.from_records(values[1:], columns=values[0])
        df_old_values['created_at'] = pd.to_datetime(df_old_values['created_at'])

        auth = tweepy.OAuthHandler(os.environ['TWITTER_API_KEY'], os.environ['TWITTER_API_SECRET'])
        auth.set_access_token(os.environ['TWITTER_ACCESS_TOKEN'], os.environ['TWITTER_ACCESS_SECRET'])
        api = tweepy.API(auth, wait_on_rate_limit=True)

        twitter_data_path = "../data"
        os.makedirs(twitter_data_path, exist_ok=True)

        # track individual twitter accounts
        for source_name, source_id in twitter_sources.items():
            # save output as
            save_file = twitter_data_path + '/tweets_' + source_id + '.json'
            tweets = api.user_timeline(
                screen_name=source_id,
                count=200,
                include_rts=False,
                tweet_mode='extended'
            )

            all_tweets = []
            all_tweets.extend(tweets)
            oldest_id = tweets[-1].id
            while True:
                tweets = api.user_timeline(
                    screen_name=source_id,
                    count=200,
                    include_rts=False,
                    max_id=oldest_id - 1,
                    tweet_mode='extended'
                )
                if len(tweets) == 0:
                    break
                oldest_id = tweets[-1].id
                all_tweets.extend(tweets)

            with open(save_file, 'a') as tf:
                for tweet in all_tweets:
                    try:
                        tf.write('\n')
                        json.dump(tweet._json, tf)
                    except Exception as e:
                        logging.warning("Some error occurred, skipping tweet:")
                        logging.warning(e)
                        pass

        # parse tweets and store in dataframe
        df_tweets = pd.DataFrame()
        for file in os.listdir(twitter_data_path):
            if file.endswith('.json'):
                df_tweets_ = pd.read_json(os.path.join(twitter_data_path, file), lines=True)
                df_tweets = df_tweets.append(df_tweets_, ignore_index=True)

        # drop duplicates
        df_tweets = df_tweets.drop_duplicates(subset=['id'])
        df_tweets = format_df(df_tweets)

        df_tweets['relevant'] = True
        for ix, row in df_tweets.iterrows():
            # skip if link already present in google sheet
            if not df_old_values.empty:
                if row['url'] in df_old_values['url'].unique():
                    df_tweets.at[ix, 'relevant'] = False

            if row['created_at'].date() < datetime.date.fromisoformat('2023-02-06'):
                df_tweets.at[ix, 'relevant'] = False

            # filter by location
            if not ('syria' in row['full_text'] or 'سوريا' in row['full_text']):
                df_tweets.at[ix, 'relevant'] = False

            # filter by keyword
            if not any(keyword.lower() in row['full_text'].lower() for keyword in keywords):
                df_tweets.at[ix, 'relevant'] = False
        df_tweets = df_tweets[df_tweets['relevant']].drop(columns=['relevant'])

        df_tweets = df_tweets.sort_values(by='created_at')
        df_tweets['created_at'] = df_tweets['created_at'].astype(str)
        df_tweets = df_tweets.fillna('')

        # add entries to google sheet
        logging.info('updating Google sheet')
        for ix, row in df_tweets.iterrows():
            # add new row to google sheet
            body = {'values': [list(row.values)]}
            result = service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id, range=spreadsheet_range,
                valueInputOption="USER_ENTERED", insertDataOption="INSERT_ROWS", body=body).execute()
            sleep(1)

    except Exception as e:
        logging.error(f"{e}")

    logging.info("Python timer trigger function ran at %s", utc_timestamp)


if __name__ == "__main__":
    main()
