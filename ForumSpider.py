#!/usr/bin/env python
# coding: utf-8
from __future__ import unicode_literals

import datetime
import logging
import re
from datetime import date as date_
from datetime import datetime, time, timedelta

from grab import Grab
from grab.spider import Spider, Task
from grab.tools.logs import default_logging
from mongoengine import *

PROXY = 'proxy.txt'
CACHE_DATABASE_NAME = '_mongo_cache'

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger()


connect('forum_test', host='184.82.222.137')


class Category(Document):
    name = StringField(max_length=100, required=True)


class User(Document):
    username = StringField(max_length=100, required=True)
    userfrom = StringField(max_length=100)
    date = DateTimeField()
    post_number = IntField(required=True)
    usertitle = IntField(required=True)
    raiting = IntField(required=True)

    # ReferenceField EmbeddedDocumentField


class Topic(Document):
    name = StringField(required=True)
    idt = IntField(min_value=1, required=True)


class Post(Document):
    author = ReferenceField(User)
    category = ReferenceField(Category)
    topic = ReferenceField(Topic)

    idp = IntField(min_value=1, required=True)
    date = DateTimeField()
    html = StringField()
    text = StringField()


class ForumSpider(Spider):

    def task_generator(self):
        for url in self.init_urls:
            yield Task('category_count', url=url)

    def task_category_count(self, grab, task):
        try:
            for i, x in enumerate(range(1, grab.doc.select('//a[@class="page"]')[-1].number())):
                url = grab.response.url + '?page=' + str(i)
                grab.setup(url=url)
                yield Task('category', grab=grab)
        except:
            print grab.response.url, 72, "Category have not more than 1 page!"

    def task_category(self, grab, task):
        for x in grab.doc.select('//div[@class="tclcon"]/a'):
            url = x.attr('href')
            category = x.text()
            category = Category.objects(
                name=category) or Category(name=category)
            grab.setup(url=url)
            yield Task('page', grab=grab, category=category)

    def task_page(self, grab, task):
        category = task.get('category')
        topic_name = grab.doc.select(
            '//div[@class="linkst"]/.//div[@class="inbox"]/ul/li')[2].text()[2:]
        topic = Topic.objects(name=topic_name) or Topic(
            name=topic_name,
            idt=grab.response.url.split('/')[-2]
        ).save()
        for block in grab.doc.select('//div[starts-with(@id, "p")]'):
            username = block.select('.//strong[@class="username"]').text()
            try:
                userfrom = block.select(
                    './/div[@class="postleft"]/.//dl/dd')[-5].text().split(': ')[1]
            except:
                userfrom = ""
            date = block.select('.//div[@class="postleft"]/.//dl/dd')
            date = datetime(
                *map(int, block.select('.//div[@class="postleft"]/.//dl/dd')[-4].text().split(': ')[1].split('-')))
            user = User.objects(username=username) or User(
                username=username,
                usertitle=re.search(
                    r"(\d)", block.select('.//dd[@class="usertitle"]/img').attr('src')).group(),
                raiting=int(
                    block.select(
                        './/div[@class="postleft"]/dl/dd[not(@class)]/strong').text()),
                userfrom=userfrom,
                date=date,
                post_number=int(block.select('.//dl/dd')
                                [-3].text().split(': ')[1])
            ).save()

            Post(
                idp=block.select('.//a[@name]').attr("name")[5:],
                date=parse_date(block.select('.//h2/span/a').text()),  # post
                post_number=block.select(
                    './/div[@class="postleft"]/.//dl/dd')[5].text().split(': ')[1],
                text=block.select('//p[@class="post_body_html"]').text(),
                html=block.select('//p[@class="post_body_html"]').html()
            ).save()

        next_page = grab.doc.select('//a[text()="Ctrl →"]')
        if next_page:
            url = next_page[0].attr('href')
            grab.setup(url=url)
            yield Task('page', grab=grab)


def parse_date(date):
    """
    вчера 15:28:07
    сегодня 12:08:50
    Ноя. 13, 2013 11:17:30
    в кошерный datetime
    """
    combine = datetime.combine

    if date.startswith("вчера"):
        return combine(date.today() - timedelta(days=1), time(map(int, date.split(' ')[1].split(':'))))

    if date.startswith("сегодня"):
        hour, minute, sec = map(int, date.split(' ')[1].split(':'))
        return combine(date.today(), time(map(int, date.split(' ')[1].split(':'))))

    date = date.split(' ')
    month = 1
    months = ['Янв.', 'Фев.', 'Март',
              'Апрель', 'Май', 'Июль', 'Июнь', 'Авг.', 'Сен.', 'Окт.', 'Ноя.', 'Дек.']
    for i, m in enumerate(months):
        if m == date[0]:
            month = i + 1
            break
    return datetime(int(date[2]), month, int(date[1][:-1]), *map(int, date[3].split(':')))


def main():
    grab = Grab()
    grab.go('http://python.su/forum/')

    bot = ForumSpider(
        thread_number=3,
        network_try_limit=2,
    )
    bot.init_urls = [x.attr('href') for x in grab.doc.select('//h3/a')]
    bot.base_url = 'http://python.su/forum'
    bot.setup_cache(backend='mongo', database=CACHE_DATABASE_NAME)
    #bot.load_proxylist(source='/tmp/'+PROXY, proxy_type='http')
    try:
        bot.run()
    except KeyboardInterrupt:
        pass

    print bot.render_stats()

if __name__ == '__main__':
    main()
