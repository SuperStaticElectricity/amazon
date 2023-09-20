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
        self.scheduler_name = "amazon_search_product"
        self.redis_key_todo = "(amazon)list_amazon_search_product:TODO"
        self.monitor_redis = False
        self.position = scheduler_info['position']


def main():
    scheduler = ItemDetailScheduler()
    sql = 'select cate2 as keyword,1 as page from db_douyin.dy_jiaju_keyword'
    scheduler.execute(sql=sql, version=version)


if __name__ == '__main__':
    main()
