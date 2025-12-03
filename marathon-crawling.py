import os
import json
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from datetime import date, datetime, timezone, timedelta
import pandas as pd
import time
import re
from tqdm import tqdm
import pymysql
import logging
from sqlalchemy import create_engine, Table, Column, Integer, Text, DateTime, Boolean, MetaData, Index, UniqueConstraint, String
from sqlalchemy.sql import select
from dotenv import load_dotenv

load_dotenv()

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

KST = timezone(timedelta(hours=9))

# ChromeOptions 설정
options = Options()

OPTION_ARGUMENTS = json.loads(os.environ.get('OPTION_ARGUMENTS'))

for arg in OPTION_ARGUMENTS:
   options.add_argument(arg)

# ChromeDriver 경로 설정: 환경 변수가 있으면 사용, 없으면 webdriver-manager 사용
CHROMEDRIVER_PATH = os.environ.get('CHROMEDRIVER_PATH')
if CHROMEDRIVER_PATH:
    logger.info(f'Using ChromeDriver from environment variable: {CHROMEDRIVER_PATH}')
    service = Service(CHROMEDRIVER_PATH)
else:
    logger.info('Using ChromeDriver from webdriver-manager')
    from webdriver_manager.chrome import ChromeDriverManager
    service = Service(ChromeDriverManager().install())

driver = webdriver.Chrome(service=service, options=options)
links = []
details = []

header_dictionary = {"대회명": "title", "대표자명": "owner",
                    "E-mail": "email", "대회일시": "schedule",
                    "전화번호": "contact", "대회종목": "course",
                    "대회지역": "location", "대회장소": "venue",
                    "주최단체": "host", "접수기간": "duration",
                    "홈페이지": "homepage", "대회장": "venue_detail",
                    "기타소개": "remark"}

try:
    driver.get("http://www.roadrun.co.kr/schedule/list.php")
    # WebDriverWait를 사용하여 특정 요소가 나타날 때까지 최대 5초간 기다립니다.
    WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.XPATH, "//font[@size='3']/a[1]")))
    # 대회 상세 페이지 링크 가져오기
    detail_links = driver.find_elements(By.XPATH, "//font[@size='3']/a[1]")
    # javascript:open_window('win', 'view.php?no=9904%27,%200,%200,%20550,%20700,%200,%200,%200,%201,%200)
    for link in detail_links:
        # 정규 표현식을 사용하여 링크에서 필요한 부분을 추출합니다.
        match = re.search(r'view\.php\?no=\d+', link.get_attribute("href"))

        if match:
            value = match.group(0)
            links.append("http://roadrun.co.kr/schedule/" + value)
        else:
            logger.warning("상세 페이지 link를 찾을 수 없습니다.")

    for link in tqdm(links):
        if link == "http://roadrun.co.kr/schedule/view.php?no=9982":
            break

        driver.get(link)
        # 상세 링크로 이동
        if link:
            driver.get(link)
            
            try:
                WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, "/html/body/table/tbody/tr/td/table")))
            except TimeoutException as e:
                logger.error("[error]: " + e)
                continue

            # 정보 수집
            rows = driver.find_elements(By.XPATH, "/html/body/table/tbody/tr/td/table[1]/tbody/tr")
            # 각 행(<tr>)을 순회하며 <td>의 텍스트를 추출
            detail = {}
            logger.info(f'{len(rows)}개의 데이터를 찾았습니다.')
            for row in rows:
                # 현재 행의 모든 헤더를 찾음
                headers = row.find_elements(By.XPATH, "./td[1]")
                # 현재 행의 모든 값을 찾음
                values = row.find_elements(By.XPATH, "./td[2]")

                # <th>와 <td>의 텍스트를 출력
                for header, value in zip(headers, values):
                    clean_value = re.sub(r'\n\d+km\n© NAVER Corp.', '', f"{value.text}")
                    detail[header_dictionary.get(f"{header.text}")] = clean_value.strip().lower()
                    detail['registered_at'] = datetime.now(KST).strftime("%Y-%m-%dT%H:%M:%S")
            details.append(detail)
            time.sleep(5)
        else:
            logger.warning("Detail link not found.")

    df = pd.DataFrame(details)
    df = df.drop_duplicates()
    df = df.dropna(axis=0, how="all")
    df.to_csv(os.environ.get('OUTPUT_CSV_FILE_NAME'), index=False)

    MYSQL_HOSTNAME = os.environ.get('MYSQL_HOSTNAME')
    MYSQL_PORT = os.environ.get('MYSQL_PORT', '3306')
    MYSQL_USER = os.environ.get('MYSQL_USER')
    MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD')
    MYSQL_DATABASE = os.environ.get('MYSQL_DATABASE').replace('-', '_')
    MYSQL_TABLE = os.environ.get('MYSQL_TABLE')

    connection_string = f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOSTNAME}:{MYSQL_PORT}/{MYSQL_DATABASE}'

    db = create_engine(connection_string)
    
    # 메타데이터 객체 생성
    metadata = MetaData()

    # 테이블 생성 스키마 정의
    table = Table('marathon', metadata,
                  Column('id', Integer, primary_key=True, autoincrement=True),
                  Column('title', String(1024), nullable=True),
                  Column('owner', String(1024), nullable=True),
                  Column('email', String(1024), nullable=True),
                  Column('schedule', String(1024), nullable=True),
                  Column('contact', String(1024), nullable=True),
                  Column('course', String(256), nullable=True),
                  Column('location', String(256), nullable=True),
                  Column('venue', String(1024), nullable=True),
                  Column('host', String(1024), nullable=True),
                  Column('duration', String(1024), nullable=True),
                  Column('homepage', String(1024), nullable=True),
                  Column('venue_detail', String(1024), nullable=True),
                  Column('remark', Text, nullable=True),
                  Column('registered_at', DateTime, nullable=False),
                  Index('idx__location__course', 'location', 'course'),
                  UniqueConstraint('title', 'owner', name='uk__title__owner')
                  )
    # 테이블 생성
    metadata.create_all(db)

    # 기존 데이터 확인
    with db.connect() as conn:
      existing_rows = conn.execute(select(table.c.title, table.c.owner)).fetchall()
      existing_combinations = {(row.title, row.owner) for row in existing_rows}

    # DataFrame에서 NaN 값을 None으로 변환
    df = df.where(pd.notnull(df), None)
    
    # 중복되지 않는 행 필터링
    df_to_insert = df[~df[['title', 'owner']].apply(tuple, axis=1).isin(existing_combinations)]
    
    # 삽입할 행 개수 로깅
    row_count = len(df_to_insert)
    logger.info(f'Number of rows inserted: {row_count}')

    # 데이터 삽입
    with db.connect() as conn:
      transaction = conn.begin()
      for _, row in df_to_insert.iterrows():
        try:
          conn.execute(table.insert().values(
              title=row['title'],
              owner=row['owner'],
              email=row['email'],
              schedule=row['schedule'],
              contact=row['contact'],
              course=row['course'],
              location=row['location'],
              venue=row['venue'],
              host=row['host'],
              duration=row['duration'],
              homepage=row['homepage'],
              venue_detail=row['venue_detail'],
              remark=row['remark'],
              registered_at=row['registered_at']
          ))
          logger.info(f"Succeed inserting row - title: {row['title']} & owner: {row['owner']}")
        except Exception as e:
          logger.error(f"Error inserting row {row['title']}, {row['owner']}: {e}")
      transaction.commit()
finally:
    # Clean up by closing the browser
    driver.quit()