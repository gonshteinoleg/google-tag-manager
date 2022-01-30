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
from time import sleep

# Авторизация
def GetService(api_name, api_version, scope, client_secrets_path):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
    'secret-file.json', scopes=['https://www.googleapis.com/auth/tagmanager.edit.containers',
                                            'https://www.googleapis.com/auth/tagmanager.edit.containerversions',
                                                     'https://www.googleapis.com/auth/tagmanager.publish'])
    http = credentials.authorize(http=httplib2.Http())
    service = build(api_name, api_version, http=http)
    return service
scopes = ['https://www.googleapis.com/auth/tagmanager.edit.containers',
         'https://www.googleapis.com/auth/tagmanager.edit.containerversions',
         'https://www.googleapis.com/auth/tagmanager.publish']
service = GetService('tagmanager', 'v2', scopes, 'client_secret.json')
account_path_cont = 'accounts/10845763/containers/2712611'
containers_list = service.accounts().containers().workspaces().list(parent=account_path_cont).execute()
workspace = json_normalize(containers_list['workspace'])
workspace = workspace['workspaceId'][0]
account_path = 'accounts/10845763/containers/2712611'+'/workspaces/'+workspace

# Создание новой рабочей области
service.accounts().containers().workspaces().create(parent='accounts/10845763/containers/2712611',
        body={
            'name': 'Удаление остановленных тегов',
        }).execute()
sleep(10)
tags_list = service.accounts().containers().workspaces().tags().list(parent=account_path).execute()
workspace_path = service.accounts().containers().workspaces().list(
    parent='accounts/10845763/containers/2712611').execute()['workspace'][0]['path']

# Добавление в список тегов, которые остановились более месяца назад
l = []
for element in tags_list['tag']:
    try:
        if datetime.utcfromtimestamp(int(element['scheduleEndMs'])/1000).strftime('%Y-%m-%d') < (datetime.now() - timedelta(30)).strftime('%Y-%m-%d'):
            l.append(element['tagId'])
    except:
        continue
        
# Удаление таких тегов
for element in tags_list['tag']:
    if element['tagId'] in l:
        service.accounts().containers().workspaces().tags().delete(
        path=workspace_path+'/tags/'+element['tagId']
        ).execute()
        sleep(5)
triggers_list = service.accounts().containers().workspaces().triggers().list(parent=account_path).execute()

# Список триггеров, которые используются в тегах
triggers = []
for element in tags_list['tag']:
    try:
        triggers.append(element['firingTriggerId'])
    except:
        continue
for element in tags_list['tag']:
    try:
        triggers.append(element['blockingTriggerId'])
    except:
        continue
        
# Удаление триггеров, которые не используются
for element in triggers_list['trigger']:
    if str(element['triggerId']) not in str(triggers):
        service.accounts().containers().workspaces().triggers().delete(
        path=workspace_path+'/triggers/'+element['triggerId']
        ).execute()
        sleep(5)
variables_list = service.accounts().containers().workspaces().variables().list(parent=account_path).execute()
variables_parameter = []
for element in variables_list['variable']:
    try:
        variables_parameter.append(element['parameter'])
    except:
        continue

# Удаление переменных, которые не используются в тегах, триггерах и других переменных (для этого получали параметр выше)
for element in variables_list['variable']:
    if str(element['name']) not in str(tags_list) and str(element['name']) not in str(triggers_list) and str(element['name']) not in str(variables_parameter):
        service.accounts().containers().workspaces().variables().delete(
        path=workspace_path+'/variables/'+element['variableId']
        ).execute()
        sleep(5)
        
# Создание версии
version = service.accounts().containers().workspaces().create_version(path=workspace_path).execute()

# Публикация версии
service.accounts().containers().versions().publish(path=version['containerVersion']['path']).execute()
