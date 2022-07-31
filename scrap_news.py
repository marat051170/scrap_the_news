import time
import datetime
import selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import pandas as pd
import math
from multiprocessing import Pool

from bs4 import BeautifulSoup
import requests


POOLS_NUMBER = 10
pools_count = 300
START_DATE = datetime.datetime(2022, 1, 1)
END_DATE = datetime.datetime.today() + datetime.timedelta(days=-1)



def get_ria_news_bs(news_link):
    date_text_news_row = {}
    page = requests.get(news_link)
    soup = BeautifulSoup(page.text, "html.parser")
    date_ = soup.find('div', class_='article__info-date').text
    try:
        title = soup.find('div', class_ = 'article__title').text
    except AttributeError:
        title = soup.find('h1', class_ = 'article__title').text
    else:
        title = ''
    text = soup.find('div', class_ = 'article__body js-mediator-article mia-analytics').text
    date_text_news_row.update({
        'date': date_,
        'link': news_link,
        'title': title,
        'text': text
        })
    date_text_news_row = pd.DataFrame(date_text_news_row, index=[0])
    return date_text_news_row


def ria_news(date_links):
    for date_ in date_links:
        driver = webdriver.Chrome(service=Service('C:/Users/marat/chromedriver.exe'))
        driver.maximize_window()
        date_ = date_.strftime('%Y%m%d')
        url = 'https://ria.ru/' + date_
        print(' ----------------------- ', url)
        driver.get(url)
        try:
            more_button = driver.find_element(By.XPATH, '//*[@id="content"]/div/div[1]/div/div[2]')
        except selenium.common.exceptions.NoSuchElementException:
            continue
        for i in range (0, 70):
            print(date_, ' --- ', i)
            driver.execute_script("arguments[0].click();", more_button)
            time.sleep(.5)
        links = []
        links_of_day = driver.find_elements(By.CSS_SELECTOR, 'a.list-item__title.color-font-hover-only')
        for link in links_of_day:
            if date_ in link.get_attribute('href'):
                links.append(link.get_attribute('href'))
        driver.close()

        news_of_dates = pd.DataFrame([])
        k = 1
        for link in set(links):
            if date_ in link:
                if 'sport' not in link:
                    print(k, ' --- ', len(set(links)), ' --- ', link)
                    news_of_date = get_ria_news_bs(link)
                    news_of_dates = pd.concat([news_of_dates, news_of_date], sort=False)
                    k += 1
        news_of_dates.to_excel(r'C:\Users\marat\OneDrive\Рабочий стол\scap_news\ria_news_bs\news_text_set_' + date_ + '.xlsx', index=False)
        return


def ria():
    dates = []
    [dates.append(date_) for date_ in pd.date_range(START_DATE, END_DATE)]
    set_count = math.ceil(len(dates) / pools_count)
    date_links_sets = [dates[i:i + set_count] for i in range(0, len(dates), set_count)]
    with Pool(POOLS_NUMBER) as p:
        p.map(ria_news, date_links_sets)


def main():
    ria()


if __name__ == '__main__':
    main()
