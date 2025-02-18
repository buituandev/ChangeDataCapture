from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np

# Scraping data from url
url = 'https://en.wikipedia.org/wiki/List_of_largest_companies_in_the_United_States_by_revenue'
page = requests.get(url)
soup = BeautifulSoup(page.text, 'html.parser')

# Header
table = soup.find('table', {'class': 'wikitable sortable'})
world_titles = table.find_all('th')
world_table_tiles = [title.text.strip() for title in world_titles]
df = pd.DataFrame(columns=world_table_tiles)

# Data
column_data = table.find_all('tr')
for row in column_data[1:]:
    row_data = row.find_all('td')
    individual_row_data = [data.text.strip() for data in row_data]
    length = len(df)
    df.loc[length] = individual_row_data

# Inspect result
# print(df.info())
# print(df.head())

# Clean data
df.dropna(inplace=True)
df['Revenue (USD millions)'] = df['Revenue (USD millions)'].str.replace(',', '').astype(float)
df['Employees'] = df['Employees'].str.replace(',', '').astype(int)

print(df)
print(df.dtypes)
print(df.describe())
print(df.Name)
print(df['Headquarters'])
# print(df.loc[:,'Name'])

top_companies = df.sort_values('Revenue (USD millions)', ascending=False).head(10)
print(top_companies)
high_employee_companies = df[np.array(df['Employees'] > 100000)]
print(high_employee_companies)
technology_industry = df[df['Industry'] == 'Technology']
print(technology_industry)

# Exporting data to csv at project root
df.to_csv('largest_companies_in_the_United_States.csv', index=False)
