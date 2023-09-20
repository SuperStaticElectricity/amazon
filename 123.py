import datetime
import json
from urllib.parse import quote

import pymysql
import requests
from loguru import logger
from pyquery import PyQuery as pq

from sl_proxyTool import getProxies
from bs4 import BeautifulSoup

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
        print(custom_name)
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
        content = content.text if content else None
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
            'is_helpful': is_helpful,
            'created_at': created_at,
            'updated_at': updated_at,
            "image_url": image_url,
            "video_url": video_url
        }
        data_list.append(data)
    
    return data_list
if __name__ == "__main__":
    product_id = "B07XQXZXJC"
    name = "Apple iPhone 11 Pro Max (64GB) - Space Grey"
    with open("data.html", "r", encoding="utf-8") as f:
        txt = f.read()
    data_list = parse(txt)
    print(data_list)