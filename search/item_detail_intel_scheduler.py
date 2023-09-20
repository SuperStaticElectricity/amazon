import datetime
import sys

sys.path.append("/data/my_spider")
from library.base_class.scheduler import BaseScheduler
from library.config import scheduler_info
from library.util import get_version

version = get_version()
hour = int(version[9:11])

class ItemDetailScheduler(BaseScheduler):

    def __init__(self):
        super(ItemDetailScheduler, self).__init__()
        self.project_name = "AMAZON"
        self.scheduler_name = "jiaju_item_detail"
        self.redis_key_todo = "(amazon)list_jiaju_item_detail:TODO"
        self.monitor_redis = False
        self.position = scheduler_info['position']


def main():
    scheduler = ItemDetailScheduler()
    created_at = str(datetime.datetime.now().strftime("%Y-%m-%d"))
    sql = f'select product_id, keyword,url from bxt.jiaju_amazon_product_list where (product_id,keyword) not in (select product_id,keyword from bxt.jiaju_amazon_item_detail)'
    scheduler.execute(sql=sql, version=version)



if __name__ == '__main__':
    main()
    # print(hour)