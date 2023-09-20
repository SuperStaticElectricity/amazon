1.library--公共方法，通用方法写入library
2.documen--框架文档
3.crawled--爬虫主逻辑
scheduler定时调度或者通过后台手动调度，向消息队列传入种子
spider爬虫从消息队列取种子，每个种子对应一次请求