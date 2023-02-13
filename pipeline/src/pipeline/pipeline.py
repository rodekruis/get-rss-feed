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
        spreadsheet_id = '1QmjjbzDc_T91aMsZY7BQWod_fBJgYNhPYGsuHp5Zkz8'
        service_account_info = json.load(open(f"{credentials_path}/google-service-account-template.json"))
        service_account_info['private_key_id'] = os.environ['PRIVATE_KEY_ID']
        service_account_info['private_key'] = os.environ['PRIVATE_KEY'].replace(r'\n', '\n')
        service_account_info['client_id'] = os.environ['CLIENT_ID']
        creds = service_account.Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=creds)

        # initialize the translator
        tr_en_translator = google_translate.Client(credentials=creds)
        # tr_en_translator = pipeline("translation_tr_to_en", model=f"Helsinki-NLP/opus-mt-tr-en")

        # get data already in the spreadsheet
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id,
                                                     range='Turkiye Timeline of events!A:F').execute()
        values = result.get('values', [])
        df_old_values = pd.DataFrame.from_records(values[1:], columns=values[0])

        # data sources
        sources = {
            'BBC Turkce': "https://feeds.bbci.co.uk/turkce/rss.xml",
            'Evrensel': 'https://www.evrensel.net/rss/haber.xml'
        }
        keywords = ['earthquake', 'victim', 'destruction', 'destroyed', 'damage', 'emergency', 'body', 'bodies', 'tent',
                    'collapse', 'rubble', 'survive', 'survivors']
        entries = []

        for source_name, source_url in sources.items():
            logging.info(f'Start source {source_name}')
            # get news feed
            NewsFeed = feedparser.parse(source_url)

            for entry in NewsFeed.entries:

                title = re.sub(r"<(.*)>", "", entry['title'])  # clean title (without HTML leftovers)
                title_en = tr_en_translator.translate(title, target_language="en")["translatedText"]  # translate title to english

                if 'summary' in entry.keys():
                    summary = re.sub(r"<(.*)>", "", entry['summary'])  # clean summary (without HTML leftovers)
                    summary_en = tr_en_translator.translate(summary, target_language="en")["translatedText"]  # translate summary
                else:
                    summary_en = title_en

                # filter by keyword
                if not any(keyword.lower() in title_en.lower() or keyword.lower() in summary_en.lower() for keyword in
                           keywords):
                    logging.info('This entry is not about the earthquake:')
                    logging.info(f"{title_en}")
                    logging.info(f"{summary_en}")
                    logging.info('---------------------------------------')
                    continue

                # skip if link already present in google sheet
                if entry['link'] in df_old_values['Link'].unique():
                    continue

                # create simple entry
                datetime = pd.to_datetime(entry['published'])
                entry_simple = {
                    'Date': datetime.strftime("%d/%m/%Y"),
                    'Time': datetime.strftime("%H:%M"),
                    'information': summary_en,
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
                spreadsheetId=spreadsheet_id, range='Turkiye Timeline of events!A:F',
                valueInputOption="USER_ENTERED", insertDataOption="INSERT_ROWS", body=body).execute()
            sleep(1)

    except Exception as e:
        logging.error(f"{e}")
        traceback.print_exception(*sys.exc_info())

    logging.info("Python timer trigger function ran at %s", utc_timestamp)


if __name__ == "__main__":
    main()
