import requests
from bs4 import BeautifulSoup
import pandas as pd
from airflow.models import XCom
import ssl

ssl.create_default_https_context = ssl._create_unverified_context

__all__ = ['get_books']

def get_books(**kwargs):
    daily_top200_book_list = []
    rank = 1
    category = '001001046'
    for i in range(1,3):
        url=f'https://www.yes24.com/Product/Category/DayBestSeller?pageNumber={i}&pageSize=100&categoryNumber={category}'
        response=requests.get(url,verify=False)
        if response.status_code == 200:
            html=response.text
            soup=BeautifulSoup(html, 'html.parser')
            
        else:
            print(response.status_code)
            
        book_list=soup.select('#yesBestList > li')
        
        for i, book in enumerate(book_list):
            # 가격 
            price = int(book.select_one('div > div.item_info > div.info_row.info_price > strong > em').text.replace(',', ''))

            # 할인율 
            try:
                discount_rate = int(book.select_one('div > div.item_info > div.info_row.info_price > span.txt_sale > em').text)
            except:
                discount_rate = 0 
                
            # 제목     
            title = book.select_one('div > div.item_info > div.info_row.info_name > a.gd_name').text.strip()
            
        
            # 작가 이름
            author = book.select_one('div > div.item_info > div.info_row.info_pubGrp > span.authPub.info_auth').text.strip()

            # 출판사
            publishing_house = book.select_one('div > div.item_info > div.info_row.info_pubGrp > span.authPub.info_pub > a').text.strip()
            
            # 출판일
            publication_date = book.select_one('div > div.item_info > div.info_row.info_pubGrp > span.authPub.info_date').text.strip()

            try:
                rate = float(book.select_one('div > div.item_info > div.info_row.info_rating > span.rating_grade > em').text.strip())
            except:
                rate = None
        
            try:
                # 리뷰수
                review_count = int(book.select_one('div > div.item_info > div.info_row.info_rating > span.rating_rvCount > a > em.txC_blue').text.replace(',', ''))
            except:
                review_count = 0

            # 출판 링크 
            link = "https://www.yes24.com" + book.select_one('div > div.item_info > div.info_row.info_name > a.gd_name')['href']


            daily_top200_book_list.append([rank, title, author, price, discount_rate, publishing_house, publication_date,  rate, review_count, link])
            rank += 1 

    data = pd.DataFrame(daily_top200_book_list, columns=['rank', 'title', 'author', 'price', 'discount_rate','publishing_house', 'publication_date', 'rate','review_count', 'link'])

    kwargs['ti'].xcom_push(key='data', value=data)
        