import datetime
import json

import pymysql
from bs4 import BeautifulSoup
from loguru import logger
from mitmproxy import http
from pyquery import PyQuery as pq

product_id = "B0BL873KHD"
product_name = "roborock Dyad Pro Wet and Dry Vacuum Cleaner with 17000Pa Intense Power Suction, Vanquish Wet and Dry Messes with DyadPower, Self-Cleaning & Drying System, Auto Cleaning Solution Dispenser"

class Counter:
    def response(self, flow: http.HTTPFlow) -> None:
        if product_id not in flow.request.url:
            return
        if "product-reviews" not in flow.request.url:
            return
        # 获取报文中的data字段
        txt = flow.response.text
        parse(txt)


def get_image(review_id, bs4_object):
    image_url_list = []
    i = 0
    while True:
        image_selector = f'#{review_id}-{i} > a > img'
        image_elem = bs4_object.select_one(image_selector)
        
        if image_elem:
            image_url = image_elem.get('src')
            if image_url:
                image_url_list.append(image_url.replace("_SY88", "_SL1600"))
                i += 1
            else:
                break
        else:
            break
    
    return image_url_list


def insert_review_to_database(review_data):
    # 数据库连接信息
    host = 'localhost'
    username = 'root'
    password = '123123123'
    database = 'db_spider'
    table = 't_amazon_review'

    # 建立数据库连接
    connection = pymysql.connect(host=host, user=username, password=password, database=database)
    cursor = connection.cursor()

    try:
        # 构建插入数据的SQL语句
        sql = f"INSERT INTO {table} (product_id, product_name, custom_name, score_star, date_review, spec, content, is_helpful, created_at, updated_at, image_url, video_url) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        values = (
            review_data['product_id'],
            review_data['product_name'],
            review_data['custom_name'],
            review_data['score_star'],
            review_data['date_review'],
            review_data['spec'],
            review_data['content'],
            review_data["is_helpful"],
            review_data['created_at'],
            review_data['updated_at'],
            json.dumps(review_data['image_url']),
            review_data['video_url']
        )

        # 执行SQL语句
        cursor.execute(sql, values)

        # 提交事务
        connection.commit()

        logger.info(review_data['custom_name'],"数据插入成功！")
    except Exception as e:
        # 发生错误时回滚事务
        connection.rollback()
        logger.error("数据插入失败:", str(e))
    finally:
        # 关闭数据库连接
        connection.close()
     

def parse(txt):
    if not txt:
        return False
    data_list = []

    soup = BeautifulSoup(txt, 'html.parser')
    review_list = soup.select("#cm_cr-review_list > div")

    for review in review_list:
        review_div_id = review.select_one('div.a-section.celwidget')
        review_div_id = review_div_id['id'] if review_div_id and 'id' in review_div_id.attrs else None
        custom_name = review.select_one("div div.a-profile-content > span")
        custom_name = custom_name.text if custom_name else None
        if not custom_name:
            continue
        score_star = review.select_one(f"#{review_div_id} > div:nth-child(2) > a > i")
        score_star = score_star.text if score_star else None
        #customer_review-R16UAYEZXNKG8P > div:nth-child(2) > a > i
        date_review = review.select_one("span.a-size-base.a-color-secondary.review-date")
        date_review = date_review.text if date_review else None
        spec = review.select_one("div.a-row.a-spacing-mini.review-data.review-format-strip > a")
        spec = spec.text if spec else None
        content = review.select_one("div.a-row.a-spacing-small.review-data")
        content = content.text.replace("\n","") if content else None
        is_helpful = review.select_one("div.a-row > span.cr-vote > div > span")
        is_helpful = is_helpful.text if is_helpful else None
        image_url = None
        video_url = None
        if review_div_id:
            review_id = review_div_id.split("-")[1]
            image_url = get_image(review_id, review)
            video_url = review.select_one(f"#review-video-id-{review_id}")
            if video_url:
                video_url = video_url['data-video-url']
        created_at = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        updated_at = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        data = {
            'custom_name': custom_name,
            'score_star': score_star,
            'date_review': date_review,
            'spec': spec,
            'content': content,
            'created_at': created_at,
            'updated_at': updated_at,
            "image_url": image_url,
            "video_url": video_url,
            "product_id":product_id,
            "product_name":product_name,
            "is_helpful":is_helpful
        }
        insert_review_to_database(data)

addons = [
    Counter()
]
