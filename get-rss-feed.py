import os.path
import feedparser
from googleapiclient.discovery import build
from google.oauth2 import service_account
from transformers import pipeline
import pandas as pd
from tqdm import tqdm
import re
from time import sleep
import json
# from newspaper import Article
# from newspaper.article import ArticleException
# import spacyturk
# import spacy
from dotenv import load_dotenv
credentials_path = 'credentials'
if os.path.exists(f"{credentials_path}/.env"):
    load_dotenv(dotenv_path=f"{credentials_path}/.env")

# # initialize the spaCyTurk model
# if not spacy.util.is_package("tr_floret_web_md"):
#     spacyturk.download("tr_floret_web_md")
# nlp = spacy.load("tr_floret_web_md")
# nlp.add_pipe('sentencizer')

# initialize the translator
tr_en_translator = pipeline("translation_tr_to_en", model=f"Helsinki-NLP/opus-mt-tr-en")

# initialize google sheets api
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
spreadsheet_id = '1QmjjbzDc_T91aMsZY7BQWod_fBJgYNhPYGsuHp5Zkz8'
service_account_info = json.load(open(f"{credentials_path}/google-service-account-template.json"))
service_account_info['private_key_id'] = os.environ['PRIVATE_KEY_ID']
service_account_info['private_key'] = os.environ['PRIVATE_KEY'].replace(r'\n', '\n')
service_account_info['client_id'] = os.environ['CLIENT_ID']
creds = service_account.Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)
result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range='Turkiye Timeline of events!A:F').execute()
values = result.get('values', [])
df_old_values = pd.DataFrame.from_records(values[1:], columns=values[0])

# data sources
sources = {
    'BBC Turkce': "https://feeds.bbci.co.uk/turkce/rss.xml",
    'evrensel': 'https://www.evrensel.net/rss/haber.xml'
}
keywords = ['earthquake', 'victim', 'destruction', 'destroyed', 'damage', 'emergency', 'body', 'bodies', 'tent', 'collapse',
            'rubble', 'survive', 'survivors']
entries = []

for source_name, source_url in sources.items():
    print('start source', source_name)
    # get news feed
    NewsFeed = feedparser.parse(source_url)

    for entry in NewsFeed.entries:

        title = re.sub(r"<(.*)>", "", entry['title'])
        title_en = tr_en_translator(title)[0]['translation_text']
        if 'summary' in entry.keys():
            summary = re.sub(r"<(.*)>", "", entry['summary'])
            summary_en = tr_en_translator(summary)[0]['translation_text']
        else:
            summary_en = title_en

        # filter by keyword
        if not any(keyword.lower() in title_en.lower() or keyword.lower() in summary_en.lower() for keyword in keywords):
            print(title_en)
            print(summary_en)
            print('not about earthquake')
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
            'Source+datetime': f'{source_name}, {datetime.strftime("%d/%m/%Y")} {datetime.strftime("%H:%M")}',
            'Link': entry['link'],
            'datetime': datetime
        }
        entries.append(entry_simple)

entries_sorted = sorted(entries, key=lambda d: d['datetime'])
print('updating Google sheet')
for entry in tqdm(entries_sorted):
    # add new row to google sheet
    body = {'values': [list(entry.values())[:-1]]}
    result = service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id, range='Turkiye Timeline of events!A:F',
        valueInputOption="USER_ENTERED", insertDataOption="INSERT_ROWS", body=body).execute()
    sleep(1)

# # parse article and add full text to separate sheet
# article = Article(entry['link'])
# try:
#     article.download()
#     article.parse()
# except ArticleException:
#     continue
# # translate text
# sents, sents_en = []
# for sent in nlp(article.text).sents:
#     sents.append(sent.text)
#     sents_en.append(tr_en_translator(sent.text)[0]['translation_text'])
# article_text = ' '.join(sents)
# article_en = ' '.join(sents_en)
#
# entry_details = {
#     'Date': datetime.strftime("%d/%m/%Y"),
#     'Time': datetime.strftime("%H:%M"),
#     'Author': ', '.join(article.authors),
#     'Title (tr)': article.title,
#     'Title (en)': tr_en_translator(article.title)[0]['translation_text'],
#     'Text (tr)': article_text,
#     'Text (en)': article_en,
#     'Link': entry['link']
# }
# # add new row to google sheet
# body = {'values': [list(entry_details.values())]}
# result = service.spreadsheets().values().append(
#     spreadsheetId=spreadsheet_id, range='Article details!A:H',
#     valueInputOption="USER_ENTERED", insertDataOption="INSERT_ROWS", body=body).execute()
