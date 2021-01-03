import requests
from bs4 import BeautifulSoup

url = "https://www.marketbeat.com/dividends/ex-dividend-date-list/"
response = requests.get(url)
soup = BeautifulSoup(response.text, "html.parser")


# Week Range
week_range = soup.find(id="cphPrimaryContent_ddlWeek")
# week_range = soup.find('option', selected="selected")

table = soup.find('table')
# print(week_range)
print(table)

A = []
B = []
C = []
D = []
E = []
F = []
headers = []

for row in table.findAll("tr"):
    cells = row.findAll('td')
    states = row.findAll('th') #To store second column data

    if len(states) > 1:
        headers.append(states[0].find(text=True))
        headers.append(states[1].find(text=True))
        headers.append(states[2].find(text=True))
        headers.append(states[3].find(text=True))
        headers.append(states[4].find(text=True))
        headers.append(states[5].find(text=True))

    if len(cells) > 1:
        A.append(cells[0].find(text=True))
        B.append(cells[1].find(text=True))
        C.append(cells[2].find(text=True))
        D.append(cells[3].find(text=True))
        E.append(cells[4].find(text=True))
        F.append(cells[5].find(text=True))

# Creating a DataFrame from the table
import pandas as pd
df = pd.DataFrame(A, columns=[headers[0]])
df[[headers[1]]] = B
df[[headers[2]]] = C
df[[headers[3]]] = D
df[[headers[4]]] = E
df[[headers[5]]] = F

print(df)

df.to_excel("C:/Users/J/Desktop/output.xlsx", sheet_name = "Sheet1")
