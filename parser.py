import requests
from bs4 import BeautifulSoup
import pandas as pd

# Angry Citizen parser
#
# col = ['title', 'description', 'district', 'username', 'datetime']
#
# row_in_dataframe = 0
# new_data = pd.DataFrame(columns=col)
#
# req = requests.get('https://www.angrycitizen.ru/problems').text
# soup = BeautifulSoup(req, 'lxml')
# links = soup.find_all('a', {'class': 'inner-link-kp'})
# for link in links:
#     if row_in_dataframe >= 1000:
#         break
#     link_req = requests.get(f"https://www.angrycitizen.ru{link['href']}").text
#     link_soup = BeautifulSoup(link_req, 'lxml')
#     titles = link_soup.find_all('a', {'class': 'what-fixed-pr'})
#     descriptions = link_soup.find_all('div', {'class': 'rating-kp-text-left'})
#     districts = link_soup.find_all('div', {'class': 'text-pr-kp'})
#     usernames = link_soup.find_all('a', {'class': 'name-fixed-pr'})
#     datetimes = link_soup.find_all('div', {'class': 'float-left-fixed-pr'})
#     for title, description, district, username, datetime in zip(titles, descriptions, districts, usernames, datetimes):
#         if row_in_dataframe >= 1000:
#             break
#         dt = datetime.find_all('span')
#         time = dt[0].text
#         date = dt[1].text
#
#         title = title.text
#         description = description.text
#         district = district.find('span').text
#         username = username.text
#         datetime = f'{date} {time}'
#
#         new_data.at[row_in_dataframe, 'title'] = title
#         new_data.at[row_in_dataframe, 'description'] = description
#         new_data.at[row_in_dataframe, 'district'] = district
#         new_data.at[row_in_dataframe, 'username'] = username
#         new_data.at[row_in_dataframe, 'datetime'] = datetime
#
#         row_in_dataframe += 1
#
# new_data.to_excel('angry_citizen.xlsx', index=False)

# Kuzbass-Online parser

# col = ['category', 'description', 'username', 'date']
#
# row_in_dataframe = 0
# new_data = pd.DataFrame(columns=col)
#
# for page_id in range(1, 126):
#     url = f'https://kemerovo.kuzbass-online.ru/proposals?cities%5B0%5D=1352&cities%5B1%5D=2&page={page_id}'
#     req = requests.get(url).text
#     soup = BeautifulSoup(req, 'lxml')
#     categories = soup.find_all('h3', {'class': 'message-card__title'})
#     descriptions = soup.find_all('p', {'class': 'message-card__text'})
#     usernames = soup.find_all('span', {'class': 'message-card__author'})
#     dates = soup.find_all('span', {'class': 'message-card__published'})
#     for category, description, username, date in zip(categories, descriptions, usernames, dates):
#         category = category.text
#         description = description.text
#         username = list(username.text)
#         date = str(date).split("('")
#         date = date[1].split("',")
#         date = date[0]
#         parsed_username = []
#         for item in username:
#             if item != ' ' and item != '\n':
#                 parsed_username.append(item)
#         username = ''.join(parsed_username)
#
#         new_data.at[row_in_dataframe, 'category'] = category
#         new_data.at[row_in_dataframe, 'description'] = description
#         new_data.at[row_in_dataframe, 'username'] = username
#         new_data.at[row_in_dataframe, 'date'] = date
#
#         row_in_dataframe += 1
#
# new_data.to_excel('kuzbass_online.xlsx', index=False)

# Narod Expert parser

from dateutil import parser

url1 = 'https://narod-expert.ru/edw/api/data-marts/435/entities.json?_=28307519&terms=3577&view_component=particular_problem_ymap&limit=250&offset=0'
url2 = 'https://narod-expert.ru/edw/api/data-marts/435/entities.json?_=28307526&terms=3577&view_component=particular_problem_ymap&limit=250&offset=250&_dm=435'
url3 = 'https://narod-expert.ru/edw/api/data-marts/435/entities.json?_=28307528&terms=113,120,121,1335,134,1667,1668,1669,1670,1675,1681,3577,77,788,789,790,813,815,9&view_component=particular_problem_ymap&limit=250&offset=500&_dm=435&_dm=435'
url4 = 'https://narod-expert.ru/edw/api/data-marts/435/entities.json?_=28307528&terms=113,120,121,1335,134,1667,1668,1669,1670,1675,1681,3577,77,788,789,790,813,815,9&view_component=particular_problem_ymap&limit=250&offset=750&_dm=435&_dm=435'

col = ['theme', 'description', 'district', 'username', 'date']

row_in_dataframe = 0
new_data = pd.DataFrame(columns=col)

resp1 = requests.get(url1)
resp_dict1 = resp1.json()
for item in resp_dict1['results']['objects']:
    item_url = f"https://narod-expert.ru{item['extra']['url']}"
    req = requests.get(item_url).text
    soup = BeautifulSoup(req, 'lxml')

    theme = item['entity_name']
    description = soup.find('div', {'class': 'description margin-bottom-20'})
    description = description.find_all('p')
    if len(description) >= 2:
        if description[1].find('span') is not None:
            description = description[1].find('span').text
    else:
        description = ''
    district = item['extra']['address']
    username = item['extra']['author']
    date = parser.parse(item['extra']['created_at']).date()

    new_data.at[row_in_dataframe, 'theme'] = theme
    new_data.at[row_in_dataframe, 'description'] = description
    new_data.at[row_in_dataframe, 'district'] = district
    new_data.at[row_in_dataframe, 'username'] = username
    new_data.at[row_in_dataframe, 'date'] = date

    row_in_dataframe += 1

resp2 = requests.get(url2)
resp_dict2 = resp2.json()
for item in resp_dict2['results']['objects']:
    item_url = f"https://narod-expert.ru{item['extra']['url']}"
    req = requests.get(item_url).text
    soup = BeautifulSoup(req, 'lxml')

    theme = item['entity_name']
    description = soup.find('div', {'class': 'description margin-bottom-20'})
    description = description.find_all('p')
    if len(description) >= 2:
        if description[1].find('span') is not None:
            description = description[1].find('span').text
    else:
        description = ''
    district = item['extra']['address']
    username = item['extra']['author']
    date = parser.parse(item['extra']['created_at']).date()

    new_data.at[row_in_dataframe, 'theme'] = theme
    new_data.at[row_in_dataframe, 'description'] = description
    new_data.at[row_in_dataframe, 'district'] = district
    new_data.at[row_in_dataframe, 'username'] = username
    new_data.at[row_in_dataframe, 'date'] = date

    row_in_dataframe += 1

resp3 = requests.get(url3)
resp_dict3 = resp3.json()
for item in resp_dict3['results']['objects']:
    item_url = f"https://narod-expert.ru{item['extra']['url']}"
    req = requests.get(item_url).text
    soup = BeautifulSoup(req, 'lxml')

    theme = item['entity_name']
    description = soup.find('div', {'class': 'description margin-bottom-20'})
    description = description.find_all('p')
    if len(description) >= 2:
        if description[1].find('span') is not None:
            description = description[1].find('span').text
    else:
        description = ''
    district = item['extra']['address']
    username = item['extra']['author']
    date = parser.parse(item['extra']['created_at']).date()

    new_data.at[row_in_dataframe, 'theme'] = theme
    new_data.at[row_in_dataframe, 'description'] = description
    new_data.at[row_in_dataframe, 'district'] = district
    new_data.at[row_in_dataframe, 'username'] = username
    new_data.at[row_in_dataframe, 'date'] = date

    row_in_dataframe += 1

resp4 = requests.get(url4)
resp_dict4 = resp4.json()
for item in resp_dict4['results']['objects']:
    item_url = f"https://narod-expert.ru{item['extra']['url']}"
    req = requests.get(item_url).text
    soup = BeautifulSoup(req, 'lxml')

    theme = item['entity_name']
    description = soup.find('div', {'class': 'description margin-bottom-20'})
    description = description.find_all('p')
    if len(description) >= 2:
        if description[1].find('span') is not None:
            description = description[1].find('span').text
    else:
        description = ''
    district = item['extra']['address']
    username = item['extra']['author']
    date = parser.parse(item['extra']['created_at']).date()

    new_data.at[row_in_dataframe, 'theme'] = theme
    new_data.at[row_in_dataframe, 'description'] = description
    new_data.at[row_in_dataframe, 'district'] = district
    new_data.at[row_in_dataframe, 'username'] = username
    new_data.at[row_in_dataframe, 'date'] = date

    row_in_dataframe += 1

new_data.to_excel('narod_expert.xlsx', index=False)


# Sakhalin Online parser

# from dateutil import parser
#
# col = ['description', 'submit_date', 'category', 'theme', 'district']
#
# row_in_dataframe = 0
# new_data = pd.DataFrame(columns=col)
# for page_id in range(0, 101):
#     data = {"orderBy": "submitDate", "orderByDirection": "DESC", "publicStatuses": []}
#     url = f'https://xn--80aayllt3a.xn--80asehdb/api/complaint-service/external/complaints/list?pageNo={page_id}&pageSize=10&orderBy=submitDate&orderByDirection=DESC'
#     resp = requests.post(url, json=data)
#     resp_dict = resp.json()
#     for item in resp_dict['items']:
#         description = item['content']
#         submit_date = parser.parse(item['submitDate']).date()
#         category = item['subject']['category']['name']
#         theme = item['subject']['name']
#         district = item['address']
#
#         new_data.at[row_in_dataframe, 'description'] = description
#         new_data.at[row_in_dataframe, 'submit_date'] = submit_date
#         new_data.at[row_in_dataframe, 'category'] = category
#         new_data.at[row_in_dataframe, 'theme'] = theme
#         new_data.at[row_in_dataframe, 'district'] = district
#
#         row_in_dataframe += 1
#
# new_data.to_excel('sakhalin_online.xlsx', index=False)

# Sevastopol Government parser

# col = ['title', 'address', 'status', 'datetime', 'category']
#
# row_in_dataframe = 0
# new_data = pd.DataFrame(columns=col)
#
# url = 'https://igrajdanin.ru/frame_ext/list_problem.php?problems=Севастополь&first=1&datefrom=10.12.2016&SHOWALL_1=1'
# req = requests.get(url).text
# soup = BeautifulSoup(req, 'lxml')
# trs = soup.find_all('tr')
# for tr in trs:
#     if row_in_dataframe >= 1000:
#         break
#     if tr == trs[0]:
#         continue
#     tds = tr.find_all('td')
#     title = tds[0].find('a').text
#     address = tds[1].text
#     status = tds[2].text
#     datetime = tds[3].text
#     category = tds[4].text
#
#     new_data.at[row_in_dataframe, 'title'] = title
#     new_data.at[row_in_dataframe, 'address'] = address
#     new_data.at[row_in_dataframe, 'status'] = status
#     new_data.at[row_in_dataframe, 'datetime'] = datetime
#     new_data.at[row_in_dataframe, 'category'] = category
#
#     row_in_dataframe += 1
#
# new_data.to_excel('sevastopol_government.xlsx', index=False)
