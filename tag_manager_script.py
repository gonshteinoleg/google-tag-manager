import argparse
import sys

import httplib2

from googleapiclient.discovery import build
from oauth2client import client
from oauth2client import file
from oauth2client import tools
from oauth2client.service_account import ServiceAccountCredentials

import pandas as pd
import json
from pandas.io.json import json_normalize

from google.cloud import bigquery

import warnings
warnings.filterwarnings('ignore')

from datetime import datetime, timedelta

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders



# Авторизация
def GetService(api_name, api_version, scope, client_secrets_path):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
    'secret-file.json', scopes=['https://www.googleapis.com/auth/tagmanager.edit.containers'])
    http = credentials.authorize(http=httplib2.Http())

    service = build(api_name, api_version, http=http)

    return service

scope = ['https://www.googleapis.com/auth/tagmanager.edit.containers']
service = GetService('tagmanager', 'v2', scope, 'client_secret.json')
account_path_cont = 'accounts/10845763/containers/2712611'

containers_list = service.accounts().containers().workspaces().list(parent=account_path_cont).execute()
workspace = json_normalize(containers_list['workspace'])
workspace = workspace['workspaceId'][0]

account_path = 'accounts/10845763/containers/2712611'+'/workspaces/'+workspace

# Получаем теги
tags_list = service.accounts().containers().workspaces().tags().list(parent=account_path).execute()
tags = json_normalize(tags_list['tag'])

# Получаем триггеры
triggers_list = service.accounts().containers().workspaces().triggers().list(parent=account_path).execute()
triggers = json_normalize(triggers_list['trigger'])

# Получаем переменные
variables_list = service.accounts().containers().workspaces().variables().list(parent=account_path).execute()
variables = json_normalize(variables_list['variable'])

# Получаем папки
folders_list = service.accounts().containers().workspaces().folders().list(parent=account_path).execute()
folders = json_normalize(folders_list['folder'])

# Преобразовываем даты активации из UNIX в datetime
tags['scheduleStartMs'] = tags['scheduleStartMs'].fillna(0)
tags['scheduleEndMs'] = tags['scheduleEndMs'].fillna(0)
tags['scheduleStartMs'] = tags['scheduleStartMs'].map(lambda x: str(x)[:-3])
tags['scheduleEndMs'] = tags['scheduleEndMs'].map(lambda x: str(x)[:-3])
tags['scheduleStartMs'] = tags['scheduleStartMs'].str.replace(" ","")
tags['scheduleEndMs'] = tags['scheduleEndMs'].str.replace(" ","")

tags['start_time'] = pd.to_datetime(tags['scheduleStartMs'],unit='s')
tags['end_time'] = pd.to_datetime(tags['scheduleEndMs'],unit='s')

# Заменяем id триггеров их названиями
tags['firingTriggerId'] = tags['firingTriggerId'].fillna(0)
tags['blockingTriggerId'] = tags['blockingTriggerId'].fillna(0)

tags['firingTriggerId'] = tags['firingTriggerId'].map(lambda x: str(x)[:-1])
tags['firingTriggerId'] = tags['firingTriggerId'].map(lambda x: str(x)[1:])
tags['firingTriggerId'] = tags['firingTriggerId'].str.replace(" ","")

tags['blockingTriggerId'] = tags['blockingTriggerId'].map(lambda x: str(x)[:-1])
tags['blockingTriggerId'] = tags['blockingTriggerId'].map(lambda x: str(x)[1:])
tags['blockingTriggerId'] = tags['blockingTriggerId'].str.replace(" ","")

triggers_list = triggers[['name', 'triggerId']].set_index('triggerId').to_dict()['name']

tags['firingTriggerId'] = tags['firingTriggerId'].replace(pd.Series(triggers_list).astype(str), regex=True)
tags['blockingTriggerId'] = tags['blockingTriggerId'].replace(pd.Series(triggers_list).astype(str), regex=True)

# Получаем eventCategory, eventAction, eventLabel и dimension
df_parameters = tags[['name','parameter']]
df_parameters['parameter'] = df_parameters['parameter'].astype(str)

df_parameters['eventCategory'] = df_parameters['parameter'].str.extract('(eventCategory.*)}')
df_parameters['eventCategory'] = df_parameters['eventCategory'].str.split("'").str[4]

df_parameters['eventAction'] = df_parameters['parameter'].str.extract('(eventAction.*)}')
df_parameters['eventAction'] = df_parameters['eventAction'].str.split("'").str[4]

df_parameters['eventLabel'] = df_parameters['parameter'].str.extract('(eventLabel.*)}')
df_parameters['eventLabel'] = df_parameters['eventLabel'].str.split("'").str[4]

df_parameters['dimension'] = df_parameters['parameter'].str.extract("(dimension', 'value.*)}")
df_parameters['dimension'] = df_parameters['dimension'].str.split("'").str[4]

del df_parameters['parameter']

# Собираем конечный датафрейм
folders = folders.rename(columns={'name':'folder', 'folderId':'parentFolderId'})
df = tags.copy()
df = df.merge(folders[['folder', 'parentFolderId']], how='left', on='parentFolderId')
df['folder'] = df['folder'].fillna('Папка не выбрана')

tags_df = df[['name', 'parameter', 'parentFolderId', 'paused', 'priority.type', 'priority.value', 'setupTag',
        'tagFiringOption', 'tagId', 'type', 'notes', 'start_time', 'end_time',
        'firingTriggerId', 'blockingTriggerId', 'folder']]

tags_df = pd.merge(tags_df, df_parameters, on='name', how='left')
tags_df = tags_df.rename(columns={'priority.type':'priorityType','priority.value':'priotityValue'})

# Триггеры
triggers_df = triggers[['name', 'triggerId', 'type', 'filter', 'customEventFilter']]

# Переменные
variables_df = variables[['name','variableId','type','parameter']]

# Преобразование параметров в строки
tags_df['parameter'] = tags_df['parameter'].astype(str)
tags_df['setupTag'] = tags_df['setupTag'].astype(str)
variables_df['parameter'] = variables_df['parameter'].astype(str)
triggers_df['filter'] = triggers_df['filter'].astype(str)
triggers_df['customEventFilter'] = triggers_df['customEventFilter'].astype(str)

# Обновление таблиц
client = bigquery.Client.from_service_account_json(
    'secret-file.json')
dataset_ref = client.dataset('tag_manager') # Определяем датасет
dataset = bigquery.Dataset(dataset_ref)

job_config = bigquery.job.LoadJobConfig()
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

# Обновляем теги
table_tags = dataset_ref.table('tags')
update_tags = client.load_table_from_dataframe(tags_df, table_tags, job_config=job_config).result()

# Обновляем переменные
table_variables = dataset_ref.table('variables')
update_variables = client.load_table_from_dataframe(variables_df, table_variables, job_config=job_config).result()

# Обновляем триггеры
table_triggers = dataset_ref.table('triggers')
update_triggers = client.load_table_from_dataframe(triggers_df, table_triggers, job_config=job_config).result()

# Отправка алертов о скором отключении тегов
end_time = tags_df[['name', 'end_time']]
end_time['end_time'] = end_time['end_time'].fillna(0)
end_time = end_time.query('end_time != 0')
end_time['end_time'] = pd.to_datetime(end_time['end_time'])
end_time['end_date'] = end_time['end_time'].dt.date
end_time['end_date'] = end_time['end_date'].astype(str)
end_time['end_date'] = end_time['end_date'].replace(" ","")

today = (datetime.now() + timedelta(14)).strftime('%Y-%m-%d')

end_time = end_time.loc[end_time['end_date'] == today]
end_time = end_time[['name']]

emails = ['gonshteinoleg@gmail.com']

if len(end_time) > 0:
    for element in emails:
        def py_mail(SUBJECT, BODY, TO, FROM):

            MESSAGE = MIMEMultipart('alternative')
            MESSAGE['subject'] = SUBJECT
            MESSAGE['To'] = TO
            MESSAGE['From'] = FROM


            # Record the MIME type text/html.
            HTML_BODY = MIMEText(BODY, 'html')

            # Attach parts into message container.
            # According to RFC 2046, the last part of a multipart message, in this case
            # the HTML message, is best and preferred.
            MESSAGE.attach(HTML_BODY)

            # The actual sending of the e-mail
            server = smtplib.SMTP('smtp.gmail.com:587')

            # Print debugging output when testing
            if __name__ == "__main__":
                server.set_debuglevel(1)

            # Credentials (if needed) for sending the mail
            password = "password"

            server.starttls()
            server.login(FROM,password)
            server.sendmail(FROM, [TO], MESSAGE.as_string())
            server.quit()


        if __name__ == "__main__":

            email_content = 'Теги, которые будут отключены ' + today + end_time.to_html()

            TO = element
            FROM ='address@gmail.com'

            py_mail("Теги, которые будут отключены через 2 недели", email_content, TO, FROM)
