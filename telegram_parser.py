import time
import pytz
from telethon.sync import TelegramClient
import pandas as pd
from telethon.tl.custom.sendergetter import SenderGetter
from telethon import functions
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import InputPeerEmpty

api_id = "api_id"
api_hash = 'api_hash'
phone = 'phone'
client = TelegramClient(phone, api_id, api_hash)
client.start()

message_text = []
message_time = []
username = []
gmt3 = pytz.timezone('Europe/Moscow')


group = client.get_entity('t.me/gorodmosru')
participants = client.get_participants(group)
users = {}
for participant in participants:
    if participant.last_name:
        last_name = participant.last_name
    else:
        last_name = ""
    if participant.first_name:
        first_name = participant.first_name
    else:
        first_name = ""
    if participant.username:
        username = participant.username
    else:
        username = ""
    if username != "":
        users[participant.id] = f'{first_name} {last_name} @{username}'
    else:
        users[participant.id] = f'{first_name} {last_name} {username}'

messages = client.iter_messages(group)
col = ['Дата и время сообщения', 'Имя пользователя', 'Текст сообщения']

row_in_dataframe = 0
new_data = pd.DataFrame(columns=col)

count = 1
for message in messages:
    if row_in_dataframe < 5000:
        user_dict = message.to_dict()['from_id']
        if user_dict is not None:
            user_id = user_dict.get('user_id')
            user = users.get(user_id)
        else:
            user = ''
        mess = message.text
        date = message.date.astimezone(gmt3)
        str_dt = str(date)
        new_data.at[row_in_dataframe, 'Дата и время сообщения'] = str_dt
        new_data.at[row_in_dataframe, 'Имя пользователя'] = user
        new_data.at[row_in_dataframe, 'Текст сообщения'] = mess
        print(row_in_dataframe)
        row_in_dataframe += 1
    else:
        new_data.to_excel(f'Название группы {count}.xlsx', index=False)
        row_in_dataframe = 0
        count += 1
        if count == 3:
            break

