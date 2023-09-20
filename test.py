# import datetime
# import json
# from urllib.parse import quote

# import pymysql
# from bs4 import BeautifulSoup, Comment
# from fake_useragent import UserAgent
# from lxml import etree
# from pyquery import PyQuery as pq


# def get_image(review_id,pq_object):
#     image_url_list = []
#     i = 0
#     while True:
#         image_selector = f'#{review_id}-{i} > a > img'    
#         image = (pq_object(image_selector).attr('src'))
#         if image:
#             image_url_list.append(image.replace("_SY88","_SL1600"))
#             i+=1
#         else:
#             break
#     return image_url_list

             
# def insert_review_to_database(review_data):
#     # 数据库连接信息
#     host = 'localhost'
#     username = 'root'
#     password = '123123123'
#     database = 'db_spider'
#     table = 't_amazon_review'

#     # 建立数据库连接
#     connection = pymysql.connect(host=host, user=username, password=password, database=database)
#     cursor = connection.cursor()

#     try:
#         # 构建插入数据的SQL语句
#         sql = f"INSERT INTO {table} (product_id, product_name, custom_name, score_star, briefly, date_review, spec, content, is_helpful, created_at, updated_at, image_url, video_url) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
#         values = (
#             review_data['product_id'],
#             review_data['product_name'],
#             review_data['custom_name'],
#             review_data['score_star'],
#             review_data['briefly'],
#             review_data['date_review'],
#             review_data['spec'],
#             review_data['content'],
#             review_data['is_helpful'],
#             review_data['created_at'],
#             review_data['updated_at'],
#             json.dumps(review_data['image_url']),
#             review_data['video_url']
#         )

#         # 执行SQL语句
#         cursor.execute(sql, values)

#         # 提交事务
#         connection.commit()

#         print("数据插入成功！")
#     except Exception as e:
#         # 发生错误时回滚事务
#         connection.rollback()
#         print("数据插入失败:", str(e))
#     finally:
#         # 关闭数据库连接
#         connection.close()
     

# def parse(txt):
#         if not txt:
#             return False
#         data_list = []

#         doc = pq(txt)
#         review_list = doc("#cm_cr-review_list > div").items()
#         for review in review_list:
#             custom_name = review("div div.a-profile-content > span").text()
#             if not custom_name:
#                 continue
#             score_star = review("div.a-row.a-spacing-none > i > span").text()
#             briefly = review(
#                 ".a-row.a-spacing-none > .a-size-base.review-title.a-color-base.review-title-content.a-text-bold > .cr-original-review-content").text()
#             date_review = review("span.a-size-base.a-color-secondary.review-date").text()
#             spec = review("div.a-row.a-spacing-mini.review-data.review-format-strip > a").text()
#             content = review("div.a-row.a-spacing-small.review-data").text()
#             is_helpful = review("div.a-row  > span.cr-vote > div > span").text()
#             review_div_id = review('div.a-section.celwidget').attr('id')
#             if review_div_id:
#                 review_id = review_div_id.split("-")[1]
#                 image_url = get_image(review_id,review)
#                 video_url = review(f"#review-video-id-{review_id}").attr("data-video-url")
#             else:
#                 image_url = []
#                 video_url = ""

#             created_at = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
#             updated_at = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
#             data = {
#                 'product_id': product_id,
#                 'product_name': name,
#                 'custom_name': custom_name,
#                 'score_star': score_star,
#                 'briefly': briefly,
#                 'date_review': date_review,
#                 'spec': spec,
#                 'content': content,
#                 'is_helpful': is_helpful,
#                 'created_at': created_at,
#                 'updated_at': updated_at,
#                 "image_url": image_url,
#                 "video_url": video_url
#             }
#             insert_review_to_database(data)
# if __name__ == "__main__":
#     product_id = "B0BFPNZPJK"
#     name = "Dreametech-D10-Plus-Self-Emptying-Navigation"
#     with open("data.html","r",encoding="utf-8") as f:
#         html = f.read()
#     a = parse(html)



from bs4 import BeautifulSoup

def extract_reviews_from_list(html):
    soup = BeautifulSoup(html, 'html.parser')
    reviews = []
    
    for review_section in soup.select('.a-section.celwidget'):
        review_data = {}
        
        # Basic Info
        profile_name_tag = review_section.select_one('.a-profile-name')
        review_title_tag = review_section.select_one('[data-hook="review-title"]')
        review_date_tag = review_section.select_one('[data-hook="review-date"]')
        review_body_tag = review_section.select_one('[data-hook="review-body"]')
        review_rating_tag = review_section.select_one('[data-hook="review-star-rating"] .a-icon-alt')
        
        review_data['profile_name'] = profile_name_tag.text.strip() if profile_name_tag else None
        review_data['review_title'] = review_title_tag.text.strip() if review_title_tag else None
        review_data['review_date'] = review_date_tag.text.strip() if review_date_tag else None
        review_data['review_body'] = review_body_tag.text.strip() if review_body_tag else None
        review_data['review_rating'] = review_rating_tag.text.strip() if review_rating_tag else None
        
        # Images
        review_data['images'] = [img_tag['src'] for img_tag in review_section.select('.review-image-tile-section img')]
        
        # Videos
        review_data['videos'] = [video_tag['src'] for video_tag in review_section.select('.video-block video')]
        
        reviews.append(review_data)
    
    return reviews


# Example usage
with open("/Users/dollar/Desktop/test.html","r") as f:
    html = f.read()
# html = '''...'''  # The HTML string you provided
a = review_data = extract_reviews_from_list(html)
print(a)
