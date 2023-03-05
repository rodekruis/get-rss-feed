import os.path
import feedparser
from google.cloud import translate_v2 as google_translate
from googleapiclient.discovery import build
from google.oauth2 import service_account
from transformers import pipeline
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
            'Sana (Arabic)': 'https://sana.sy/?feed=rss2',
            'Sana (English)': 'https://sana.sy/en/?feed=rss2',
            'Alwatan': 'https://alwatan.sy/feed',
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
                         "cross-border aid", "crossline operations", "cholera", "north-west Syria", "northwest Syria"]
        arabic_query = ["إدلب", "العبور الآمن", " عقوبات", "مساعدات عبر الحدود", "العمليات عبر الحدود", "الكوليرا",
                        "شمالي غربي سوريا", "إدلب", "العبور الآمن", " عقوبات", "مساعدات عبر الحدود",
                        "العمليات عبر الحدود", "الكوليرا", "شمالي غربي سوريا"]
        keywords = english_query + arabic_query

        entries = []

        for source_name, source_url in sources.items():
            logging.info(f'Start source {source_name}')
            # get news feed
            NewsFeed = feedparser.parse(source_url)

            for entry in NewsFeed.entries:
                datetime = pd.to_datetime(entry['published'])

                if not df_old_values.empty:
                    # skip if link already present in google sheet
                    if entry['link'] in df_old_values['Link'].unique():
                        continue
                    # skip if older than latest news
                    if datetime < df_old_values[df_old_values['Source'] == source_name]['datetime'].max().tz_localize(
                            'UTC+03:00'):
                        print(f"{datetime} is older than {df_old_values['datetime'].max()}, skipping")
                        continue
                    else:
                        print(f"{datetime} is newer than {df_old_values['datetime'].max()}, saving")

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
                    'Date': datetime.strftime("%d/%m/%Y"),
                    'Time': datetime.strftime("%H:%M"),
                    # 'Title (en)': title_en,
                    'Title': title,
                    # 'Content (en)': content_en,
                    'Content': content,
                    'Source': source_name,
                    'Source+datetime': f'{source_name}, {datetime.strftime("%d/%m/%Y")} {datetime.strftime("%H:%M")}',
                    'Link': entry['link'],
                    'datetime': datetime
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

        # Twitter ...
        twitter = {
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

    except Exception as e:
        logging.error(f"{e}")

    logging.info("Python timer trigger function ran at %s", utc_timestamp)


if __name__ == "__main__":
    main()
