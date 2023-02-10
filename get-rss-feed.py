import feedparser
from googleapiclient.discovery import build
from google.oauth2 import service_account
from transformers import pipeline
import pandas as pd

# initialize translator
tr_en_translator = pipeline("translation_tr_to_en", model=f"Helsinki-NLP/opus-mt-tr-en")

# initialize google sheets api
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
spreadsheet_id = '1QmjjbzDc_T91aMsZY7BQWod_fBJgYNhPYGsuHp5Zkz8'
range_name = 'Turkiye Timeline of events!A:E'
sa_file = 'credentials/google-service-account-turkey-syria-earthquake-2023.json'
creds = service_account.Credentials.from_service_account_file(sa_file, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)
result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
values = result.get('values', [])
df_old_values = pd.DataFrame.from_records(values[1:], columns=values[0])

# data sources
sources = {
    'BBC Turkce': "https://feeds.bbci.co.uk/turkce/rss.xml"
}
keywords = ['earthquake']


for source_name, source_url in sources.items():

    # get news feed
    NewsFeed = feedparser.parse(source_url)

    for entry in NewsFeed.entries:

        title_en = tr_en_translator(entry['title'])[0]['translation_text']
        summary_en = tr_en_translator(entry['summary'])[0]['translation_text']

        # filter by keyword
        if not any(keyword.lower() in title_en.lower() or keyword.lower() in summary_en.lower() for keyword in keywords):
            continue

        # skip if link already present
        if entry['link'] in df_old_values['Link'].unique():
            continue

        datetime = pd.to_datetime(entry['published'])
        entry_simple = {
            'Date': datetime.strftime("%d/%m/%Y"),
            'Time': datetime.strftime("%H:%M"),
            'information': summary_en,
            'Source': source_name,
            'Link': entry['link']
        }
        print(entry_simple)

        # add new row to google sheet
        body = {
            'values': [list(entry_simple.values())]
        }
        result = service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id, range=range_name,
            valueInputOption="USER_ENTERED", insertDataOption="INSERT_ROWS", body=body).execute()
        print(f"{result.get('updatedCells')} cells updated.")
