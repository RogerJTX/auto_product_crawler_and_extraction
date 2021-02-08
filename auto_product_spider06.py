'''
description: company product
author: jtx
date: 2019_12_01
'''

#coding=utf8

import sys,os
sys.path.append('...')


from pathlib import Path
# import lxml
# from lxml import etree
# import requests
# import sys
# import random
# import os
import re
import shutil
from lxml import etree
from bs4 import BeautifulSoup
from urllib.request import urlopen
import logging
import pymongo
# import base64
import urllib
import time, requests
import datetime, random
from etl.utils.log_conf import configure_logging
# import traceback
from etl.data_gather.settings import SAVE_MONGO_CONFIG, RESOURCE_DIR
from etl.common_spider.donwloader import Downloader
# from selenium import webdriver
from urllib.parse import urljoin
from urllib import parse
from urllib.parse import urlunparse
from posixpath import normpath
import chardet
# import html2text
# from boilerpipe.extract import Extractor
# from goose3 import Goose
# from goose3.text import StopWordsChinese
# from sklearn.feature_extraction.text import CountVectorizer
# import numpy as np
# from scipy.linalg import norm
# from pyhanlp import *
# from jpype import *
import sys
import psutil
import os
import threading

sys.setrecursionlimit(1000000)


class ListDetailSpider(object):
    def __init__(self, config, proj=None, num='01'):
        # config.................

        self.start_down_time = datetime.datetime.now()
        self.down_retry = 3
        configure_logging("...autoproduct_crawler_and_cleaning/QYGW_product_threading___"+num+".log")  # 日志文件名
        self.logger = logging.getLogger("spider")
        self.downloader = Downloader(self.logger, need_proxy=False)  # 注意是否需要使用代理更改参数
        self.headers = {
            'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:68.0) Gecko/20100101 Firefox/68.0",
            # 'Referer': '',
            # 'Host': self.host,
        }
        self.save_to_mongo = 0


    def get_mongo(self, host, port, db, username, password):
        if username and password:
            url = "mongodb://%s:%s@%s:%s/%s" % (username, password, host, port, db)
        else:
            url = "mongodb://%s:%s" % (host, port)
        return pymongo.MongoClient(url)

    def remove_punctuation_file(self, line):
        line = str(line)
        rule = re.compile(u"[^a-zA-Z0-9\u4E00-\u9FA5]")
        line = rule.sub('_', line)
        return line

    def save_record(self, records, coll_name, pk, threading_name):
        tmp = []

        for k, v in pk.items():
            tmp.append("%s=%s" % (k, v))
            # print( tmp)
        show = "  ".join(tmp)
        # self.logger.info('去重复')
        r_in_db = coll_name.find_one(pk)
        # self.logger.info('去重复结束')
        # print ('show：' + show)
        if not r_in_db:
            try:
                coll_name.insert_one(records)
                self.logger.info("成功插入(%s), threading_name:%s  %s" % (records['url'], threading_name, show))
                self.save_to_mongo += 1
            # 判断是否是下载类型的文件，下载文件过大是无法插入到mongo的
            except Exception as EE:
                if 'BSON document too large' in str(EE):
                    self.logger.info('文件过大, 无法插入, pass, url:%s threading_name:%s  %s' % (records['url'], threading_name, show))
                else:
                    self.logger.info(EE)
            # print('save:', self.save_to_mongo)
        else:
            self.logger.info("重复数据(%s), threading_name:%s  %s" % (records['url'], threading_name, show))

    def run(self, txt_name, threading_name):
        """
        数据采集主入口
        :return:
        """
        self.logger.info("Begin Run")
        c = 0
        # 数据库中获取主页url
        # for each in self.mongo_read_col1.find(no_cursor_timeout = True):
        #     c += 1
        #     self.logger.info('第%s个企业' % c)
        #     url = each['website']
        #     company_name = each['company_name']
        #     print('company_name:', company_name)
        #     print('host_url:', url)

        # for each_company in self.mongo_read_col1.find().batch_size(1):
            # 防止报错pymongo.errors.CursorNotFound: Cursor not found, cursor id: 873149949844
        self.save = 0
        recursive_time = 100000001

        self.filename_extension_upper_list = []
        self.filename_extension_lower_list = []
        with open('.../00basic/filename_extension.txt', 'r', encoding='utf-8') as f_houzhui:
            for f_houzhui_each in f_houzhui.readlines():
                word_filename_extension = f_houzhui_each.replace('\n', '')
                self.filename_extension_lower_list.append(word_filename_extension.lower())
                self.filename_extension_upper_list.append(word_filename_extension)
        # print(self.filename_extension_lower_list)
        # print(self.filename_extension_upper_list)

        with open(txt_name, 'r', encoding='utf-8') as f0:
            for each_company_f0 in f0.readlines():
                self.o = 0
                self.last_time_begin = datetime.datetime.now()
                url_list = []
                url_list_copy = []
                have_get_url_list = []
                link_linshi_list = []
                title_list = []
                link_title_filter_dict = {}
                each_company = each_company_f0.split(',')
                c += 1
                if c < 0:
                    continue
                website = each_company[1].replace('\n', '')
                company_name = each_company[0]
                self.d = 0
                self.p = 0
                self.big_url_num = 0
                # print(each_company)
                if website:
                    url = website
                    if '1688.com' in url:
                        self.logger.info('购物网站1688, pass')
                        continue
                    if url.endswith('/'):
                        url_filename_linshi = url[:-1]
                        url_filename = self.remove_punctuation_file(url_filename_linshi)
                    else:
                        url_filename = self.remove_punctuation_file(url)

                    # 判断是否存在文件夹，没有则创建
                    a = os.path.exists('...autoproduct_crawler_and_cleaning/auto_product_linshi/' + url_filename)
                    if a:
                        self.logger.info('文件夹存在')
                    else:
                        os.mkdir('...autoproduct_crawler_and_cleaning/auto_product_linshi/' + url_filename)  # 创建目录
                    with open('...autoproduct_crawler_and_cleaning/auto_product_linshi/' + url_filename + '/web_site.txt', 'a+', encoding='utf-8') as f:
                        with open('...autoproduct_crawler_and_cleaning/auto_product_linshi/' + url_filename + '/web_site_title.txt', 'a+',
                                  encoding='utf-8') as f1:
                            while len(url_list) >= 0:
                                self.p += 1
                                # print(len(url_list))
                                self.logger.info('第'+str(c)+'个企业----------threading_name:%s' % threading_name)

                                # pk1 = {"company_name": each_company}
                                # r_in_db = self.mongo_read_col3.find_one(pk1)
                                # if r_in_db:
                                #     continue

                                arr = parse.urlparse(url)
                                arr_netloc = arr.netloc  # host name
                                self.logger.info('arr_netloc:%s'% arr_netloc)
                                if url_list == []:
                                    resp = self.downloader.crawl_data(url, None, self.headers, "get")
                                    self.logger.info(resp)
                                    if resp:
                                        content_first = resp.content
                                        try:
                                            bianma_judge = chardet.detect(content_first)
                                            bianma = bianma_judge['encoding']
                                        except Exception as e:
                                            self.logger.info(e)
                                            bianma = 'utf-8'
                                        title_list.append('主页')
                                    else:
                                        self.logger.info('主页不响应')
                                        break
                                else:
                                    resp = 1
                                    url = self.i

                                if resp:
                                    url_list_copy, title_list, link_title_filter_dict, url_list, have_get_url_list, recursive_time = self.host_page_extraction(url, bianma, url_list, url_list_copy, title_list, arr_netloc, have_get_url_list, link_linshi_list, f, f1, url_filename, link_title_filter_dict, recursive_time)
                                    # url_list_copy = sorted(
                                    # url_list_copy, key=lambda i: len(i), reverse=False)
                                    if (len(url_list) == 0):
                                        break
                                    if (self.big_url_num > 5):
                                        self.logger.info('self.big_url_num > 5')
                                        break
                                    if url_list_copy == None:
                                        continue
                                    # self.logger.info(url_list_copy)
                                    self.logger.info(len(url_list_copy))

                                    # 最后主要是用url_list_copy, title_list
                                    for num, i1 in enumerate(url_list_copy):
                                        if num > self.d * 20:
                                            f.write(i1 + '\n')
                                    for num, i2 in enumerate(title_list):
                                        if num > self.d * 20:
                                            f1.write(i2 + '\n')

                                if url_list_copy:
                                    if len(url_list_copy) <= 10000:
                                        # mongo插入与删除
                                        for num, each_url_list_copy in enumerate(have_get_url_list):
                                            # try:
                                            if len(have_get_url_list) < (self.d+1)*20:
                                                if len(url_list_copy) <= 20:
                                                    self.logger.info('企业url_list_copy<=20, company_name: %s' % company_name)
                                                    pass
                                                else:
                                                    # print('ERROR:',len(have_get_url_list))
                                                    # print('len(have_get_url_list):', len(have_get_url_list))
                                                    # print('len(url_list_copy):', len(url_list_copy))
                                                    # print('第几次20轮的循环, d:', self.d)
                                                    # print('第几次请求或是递归, o:', self.o)
                                                    self.logger.info('ERROR:%s' % len(have_get_url_list))
                                                    self.logger.info('len(have_get_url_list):%s' % len(have_get_url_list))
                                                    self.logger.info('len(url_list_copy):%s' % len(url_list_copy))
                                                    self.logger.info('len(url_list):%s' % len(url_list))
                                                    self.logger.info('d:%s'% self.d)
                                                    self.logger.info('o:%s'% self.o)

                                                    self.logger.info('------------------')
                                                    self.logger.info('break: 企业error  len(have_get_url_list) < (self.d+1)*20, company_name:%s' % company_name)
                                                    break

                                            if num > self.d*20:
                                                self.logger.info('len(have_get_url_list):%s' % len(have_get_url_list))
                                                self.logger.info('d:%s'% self.d)
                                                self.logger.info('o:%s'% self.o)
                                                record = {}
                                                self.logger.info(each_url_list_copy)
                                                judge_each_url_list_copy_file = os.path.exists('.../autoproduct_crawler_and_cleaning/auto_product_linshi/'+url_filename + '/' + url_filename + '_content/' + self.remove_punctuation_file(each_url_list_copy) + '.txt')
                                                if judge_each_url_list_copy_file:
                                                    with open('...autoproduct_crawler_and_cleaning/auto_product_linshi/'+url_filename + '/' + url_filename + '_content/' + self.remove_punctuation_file(each_url_list_copy) + '.txt', 'r',encoding='utf-8') as f2:
                                                        with open('...autoproduct_crawler_and_cleaning/auto_product_linshi/'+url_filename + '/' + url_filename + '_html/' + self.remove_punctuation_file(each_url_list_copy) + '.txt', 'r', encoding='utf-8') as f3:

                                                            crawl_time = datetime.datetime.now()
                                                            title = title_list[num]
                                                            record['company_name'] = company_name
                                                            record['title'] = title
                                                            record['web_site'] = website
                                                            record['url'] = each_url_list_copy
                                                            record['html'] = f3.read()
                                                            record['content'] = f2.read()
                                                            record['crawl_time'] = crawl_time
                                                            if record:
                                                                self.save_record(record, self.mongo_read_col3, {"url": record["url"]}, threading_name)
                                                else:
                                                    self.logger.info('文件夹不存在, len(len_chinese_tag) <= 50, pass')
                                    else:
                                        myquery = {"website": website}
                                        str_big = '10000+, 过大'
                                        newvalues = {"$set": {"size":str_big}}
                                        self.mongo_read_col3.update_one(myquery, newvalues)
                                        self.logger.info('子页超过10000页，跳过，下一个企业')
                                        self.updateFile(txt_name, each_company_f0, "")
                                        a = os.path.exists('...autoproduct_crawler_and_cleaning/auto_product_linshi/' + url_filename)
                                        if a:
                                            print('文件夹存在')
                                            shutil.rmtree('...autoproduct_crawler_and_cleaning/auto_product_linshi/' + url_filename)
                                            print('文件夹已经删除')
                                        else:
                                            print('文件夹不存在')
                                        break
                                else:
                                    self.logger.info('No url_list_copy')
                                self.d += 1
                                if len(url_list_copy) - len(url_list) == 0:
                                    break

                                self.last_time_end = datetime.datetime.now()
                                time_judge = (self.last_time_begin - self.last_time_end).seconds
                                if time_judge > 18000:
                                    self.logger.info('一个企业超过5小时, pass')
                                    break


                            myquery = {"website": website}
                            newvalues = {"$set": {"size": len(url_list_copy)}}
                            self.mongo_read_col1.update_one(myquery, newvalues)
                            self.logger.info('更新了企业size %s, %s' % (company_name, website))
                            self.logger.info('此企业已经跑完, 进入下一个企业')

                a = os.path.exists('...autoproduct_crawler_and_cleaning/auto_product_linshi/' + url_filename)
                if a:
                    self.logger.info('文件夹存在')
                    shutil.rmtree('...autoproduct_crawler_and_cleaning/auto_product_linshi/' + url_filename)
                    self.logger.info('文件夹已经删除')
                else:
                    self.logger.info('文件夹不存在')
                self.logger.info("下一个企业")


                self.updateFile(txt_name, each_company_f0, "")  # 将"D:\zdz\"路径的myfile.txt文件把所有的zdz改为daziran
                self.logger.info('已删除此条数据:%s' % company_name)

                # 读取数据库大小

                count = self.mongo_read_col3.find().count()
                size_all = 2.2312626242637634e-05*count
                self.logger.info('size:%sGB' % str(size_all))
                if size_all > 800.0:
                    self.logger.info('size too large, break')
                    break


        self.logger.info("Finish Run")

    # TODO 主页的抽取
    def host_page_extraction(self, url, bianma, url_list, url_list_copy, title_list, arr_netloc, have_get_url_list, link_linshi_list, f, f1, url_filename, link_title_filter_dict,recursive_time):
        # print('---------------------------------------------每次循环/递归 分界线---------------------------------------------------------')
        self.logger.info('---------------------------------------------每次循环/递归 分界线---------------------------------------------------------')
        self.memory_info = psutil.virtual_memory()
        # print('内存占比：', self.memory_info.percent, '%')
        self.logger.info('内存占比：%s %s' % (self.memory_info.percent, '%'))
        self.o += 1
        # print(have_get_url_list)
        # print(url_list_copy)
        # 注意返回空的时候也要加入数据
        if url in have_get_url_list:
            self.logger.info('url in have_get_url_list:%s' % url)
            return url_list_copy, title_list, link_title_filter_dict, url_list, have_get_url_list, recursive_time

        if url not in have_get_url_list:
            have_get_url_list.append(url)
        if (url not in url_list_copy) and (self.o == 1):
            self.logger.info('url不存在于url_list_copy, url:%s' % url)
            title_list.append('主页')
            url_list_copy.append(url)
            link_title_filter_dict[url] = '主页'
            link_linshi_list.append(url)

        resp = self.downloader.crawl_data(url, None, self.headers, "get")
        # print(resp)

        # TODO 判断是否请求链接的文件过大
        if resp:
            content_size = resp.content
            content_size_judge = sys.getsizeof(content_size)
            content_size_judge = int(int(content_size_judge) / 1024 / 1024)
            if content_size_judge <= 3:
                pass
            else:
                self.big_url_num += 1
                self.logger.info('文件过大, pass, size:%sMB, big_url_num:%s, url:%s' % (content_size_judge, self.big_url_num, url))
                resp = ''
        if self.big_url_num >5:
            self.logger.info('self.big_url_num > 5')
            return url_list_copy, title_list, link_title_filter_dict, url_list, have_get_url_list, recursive_time


        if resp:
            resp.encoding = bianma
            html = resp.text

            # 重定向判断 判断html完整源码，在除去script部分之前
            # self.judge_whether_needs_location(html, url, url_list_copy, have_get_url_list, bianma)

            soup = BeautifulSoup(html, 'lxml')
            [s.extract() for s in soup("script")]
            content = soup.get_text().strip()

            len_chinese_tag = self.remove_punctuation_chinsese(content)
            if len(len_chinese_tag) > 50:
                # 判断是否存在文件夹，没有则创建


                self.judge_whether_exists_file(url_filename, content, html, url)

                self.logger.info('len_chinese_tag>50, html, content写入成功')

                url_list_copy, link_linshi_list, link_title_filter_dict, arr_netloc, url_list, url, title_list = \
                    self.find_all_href(soup, url_list_copy, link_linshi_list, link_title_filter_dict, arr_netloc,
                                       url_list, url, title_list)
            else:
                self.logger.info('len(len_chinese_tag) <= 50, pass')


            if self.o == 1:
                recursive_time = recursive_time + 1
            else:
                recursive_time = len(url_list_copy) - len(url_list)
            self.logger.info('recursive_time:%s' % recursive_time)

            # 判断是否
            if len(url_list) == 0:
                self.logger.info('len(url_list)=0')
                return url_list_copy, title_list, link_title_filter_dict, url_list, have_get_url_list, recursive_time

            if len(url_list) > 0:
                i = url_list[0]
                self.i = i
                self.logger.info('当前递归url, i:%s' % self.i)
                self.index = url_list.index(self.i)
                url_list.remove(self.i)
                if self.o != 1 and (recursive_time % 20 == 0):
                    return url_list_copy, title_list, link_title_filter_dict, url_list, have_get_url_list, recursive_time
                self.logger.info("url_list: %s" % len(url_list))
                self.logger.info("url_list_copy: %s" % len(url_list_copy))

                random_a = random.uniform(1.0, 2.0)
                round_a = (round(random_a, 1))
                time.sleep(round_a)

                # 调用递归函数，返回结果
                url_list_copy, title_list, link_title_filter_dict, url_list, have_get_url_list, recursive_time = self.host_page_extraction(self.i, bianma, url_list, url_list_copy, title_list, arr_netloc, have_get_url_list, link_linshi_list, f, f1, url_filename, link_title_filter_dict,recursive_time)

                # if len(url_list_copy) > 500:
                #     continue
                # return url_list_copy, title_list, link_title_filter_dict, url_list, have_get_url_list, recursive_time
                if self.o != 1 and (recursive_time % 20 == 0):
                    return url_list_copy, title_list, link_title_filter_dict, url_list, have_get_url_list, recursive_time
            else:
                return url_list_copy, title_list, link_title_filter_dict, url_list, have_get_url_list, recursive_time
        else:
            # 判断是否存在文件夹，没有则创建
            content = '页面未响应:%s' % (url)
            html = '页面未响应:%s' % (url)
            self.judge_whether_exists_file(url_filename, content, html, url)
            self.logger.info('页面未响应:%s' % (url))
            recursive_time = len(url_list_copy) - len(url_list)
            self.logger.info('recursive_time:%s' % recursive_time)

            # 这里不加这个递归会显示 None type不可迭代
            if len(url_list) > 0:
                i = url_list[0]
                self.i = i
                self.logger.info('当前递归url, i:%s' % self.i)
                self.index = url_list.index(self.i)
                url_list.remove(self.i)
                if self.o != 1 and (recursive_time % 20 == 0):
                    return url_list_copy, title_list, link_title_filter_dict, url_list, have_get_url_list, recursive_time
                self.logger.info("url_list: %s" % len(url_list))
                self.logger.info("url_list_copy: %s" % len(url_list_copy))

                random_a = random.uniform(1.0, 2.0)
                round_a = (round(random_a, 1))
                time.sleep(round_a)
                if len(url_list) == 0:
                    return url_list_copy, title_list, link_title_filter_dict, url_list, have_get_url_list, recursive_time

                # 调用递归函数，返回结果
                url_list_copy, title_list, link_title_filter_dict, url_list, have_get_url_list, recursive_time = self.host_page_extraction(self.i, bianma, url_list, url_list_copy, title_list, arr_netloc, have_get_url_list, link_linshi_list, f, f1, url_filename, link_title_filter_dict,recursive_time)

                # if len(url_list_copy) > 500:
                #     continue
                # return url_list_copy, title_list, link_title_filter_dict, url_list, have_get_url_list, recursive_time
                if self.o != 1 and (recursive_time % 20 == 0):
                    return url_list_copy, title_list, link_title_filter_dict, url_list, have_get_url_list, recursive_time
            else:
                return url_list_copy, title_list, link_title_filter_dict, url_list, have_get_url_list, recursive_time



        # TODO 重要判断-退出递归 ##################
        # 判t断是否需要退出递归
        if (len(url_list) == 0) or (self.big_url_num > 5):
            self.logger.info('len(url_list)=0, 退出递归')
            return url_list_copy, title_list, link_title_filter_dict, url_list, have_get_url_list, recursive_time



    # 判断是否需要重定向
    def judge_whether_needs_location(self, html, url, url_list_copy, have_get_url_list, bianma):
        judge_location_href, location_href = self.win_location_href(html, url)
        if judge_location_href:  # 需要重定向
            url2 = url
            url = self.url_pinjie(url2, location_href)
            self.logger.info('重定向后, url:%s' % (url))
            for num, url_change_linshi in enumerate(url_list_copy):
                if url2 == url_change_linshi:
                    url_list_copy[num] = url
                    self.logger.info('重定向后url_list_copy中的url更新成功, url:%s' % (url))

            # print(have_get_url_list)
            # print(url_list_copy)
            if url in have_get_url_list:
                return None, None
            resp = self.downloader.crawl_data(url, None, self.headers, "get")
            # print(resp)

            if url not in have_get_url_list:
                have_get_url_list.append(url)

            if resp:
                resp.encoding = bianma
                html = resp.text
        else:  # 不需要重定向
            pass


    # 判断是否存在html和content文件夹
    def judge_whether_exists_file(self, url_filename, content, html, url):
        a1 = os.path.exists('.../autoproduct_crawler_and_cleaning/auto_product_linshi/' + url_filename + '/' + url_filename + '_content')
        if a1:
            pass
        else:
            os.mkdir('.../autoproduct_crawler_and_cleaning/auto_product_linshi/' + url_filename + '/' + url_filename + '_content')  # 创建目录
        a2 = os.path.exists('.../autoproduct_crawler_and_cleaning/auto_product_linshi/' + url_filename + '/' + url_filename + '_html')
        if a2:
            pass
        else:
            os.mkdir('.../autoproduct_crawler_and_cleaning/auto_product_linshi/' + url_filename + '/' + url_filename + '_html')  # 创建目录

        # try:
        with open(
                '.../autoproduct_crawler_and_cleaning/auto_product_linshi/' + url_filename + '/' + url_filename + '_content/' + self.remove_punctuation_file(
                        url) + '.txt', 'a+', encoding='utf-8') as f2:
            f2.write(content)
        with open(
                '.../autoproduct_crawler_and_cleaning/auto_product_linshi/' + url_filename + '/' + url_filename + '_html/' + self.remove_punctuation_file(
                        url) + '.txt', 'a+', encoding='utf-8') as f3:
            f3.write(html)
        # except Exception as E1:
        #     self.logger.info(E1)
        #     return url_list_copy, title_list, link_title_filter_dict


    # 找每一个网页中的所有链接
    def find_all_href(self, soup, url_list_copy, link_linshi_list, link_title_filter_dict, arr_netloc, url_list, url, title_list):
        links = soup.find_all('a')
        for i in links:
            link = i.get('href')
            link_title = i.get_text().strip().replace('\n', '').replace('\r', '')
            # print(link)

            if link and (link not in url_list_copy) and (link not in link_linshi_list):
                if 'none' in link:
                    continue
                if len(link) < 100:
                    # link_linshi_list 是已经匹配过的 待拼接的短链接，去重复，location_href是重定向后的短链接，也加入列表，去重复
                    link_linshi_list.append(link)
                    # link_linshi_list.append(location_href)
                    link_title_filter_dict[link] = link_title
                    if ('http' in link) and (arr_netloc in link):
                        each_url = link
                        # each_map[link_title] = each_url
                        if (each_url not in url_list_copy) and (not each_url.endswith('.pdf')) and \
                                ('news' not in each_url) and ('News' not in each_url) and ('job' not in each_url) \
                                and ('about' not in each_url) and ('download' not in each_url):
                            # 调用函数pass带后缀的网址，即是下载链接的网址
                            judge_pass_filename_extension = self.pass_filename_extension(each_url)
                            if judge_pass_filename_extension:
                                if (len(link_title) < 30):
                                    # print(each_url)
                                    url_list.append(each_url)
                                    url_list_copy.append(each_url)
                                    title_list.append(link_title)
                                    self.save += 1
                                    # f.write(each_url + '\n')
                                    # f1.write(link_title + '\n')
                    else:
                        # 写try, 防止url里有中文, url.join报错
                        try:
                            each_url = self.url_pinjie(url, link)
                            if len(each_url) < 100:
                                # print('each_url:', each_url)
                                if arr_netloc in each_url:
                                    # each_map[link_title] = each_url
                                    if (each_url not in url_list_copy) and (not each_url.endswith('.pdf')) and \
                                            ('news' not in each_url) and ('News' not in each_url) and (
                                            'job' not in each_url) \
                                            and ('about' not in each_url) and ('download' not in each_url):
                                        judge_pass_filename_extension = self.pass_filename_extension(each_url)
                                        if judge_pass_filename_extension:
                                            if (len(link_title) < 30):
                                                # print('each_url:', each_url)
                                                url_list.append(each_url)
                                                url_list_copy.append(each_url)
                                                title_list.append(link_title)
                                                self.save += 1
                                                # f.write(each_url + '\n')
                                                # f1.write(link_title + '\n')
                        except Exception as E2:
                            self.logger.info('url_pinjie Error E2:%s' % E2)


                else:
                    pass

            # link_linshi_list是待拼接的url
            elif link and (link in link_linshi_list):
                if 'none' in link:
                    continue

                if len(link_title) < 30:
                    if len(link_title) > len(link_title_filter_dict[link]):
                        link_title_filter_dict[link] = link_title
                        try:
                            each_url = self.url_pinjie(url, link)
                            for num, link2 in enumerate(url_list_copy):
                                if each_url == link2:
                                    title_list[num] = link_title
                            # print('碰到相同short_url, 修改url_title成功')
                        except Exception as E3:
                            self.logger.info('url_pinjie Error E3:%s' % E3)

                else:
                    pass
        return url_list_copy, link_linshi_list, link_title_filter_dict, arr_netloc, url_list, url, title_list



    # 网页重定向href
    def win_location_href(self, html, url):
        href = ''
        if 'window.location.href' in str(html):
            # print(html)

            judge_list = str(html).split('\n')
            for i in judge_list:
                if 'window.location.href' in str(i):
                    if len(str(i)) < 70:
                        self.logger.info('页面需要重定向, url:%s, 原文："%s"' % (url, i))
                        i_list_linshi1 = re.findall(r"'(.+?)'",str(i))
                        if i_list_linshi1:
                            href = i_list_linshi1[0]
                            return True, href
                        i_list_linshi2 = re.findall(r'"(.+?)"',str(i))
                        if i_list_linshi2:
                            href = i_list_linshi2[0]
                            return True, href
                    else:
                        # print("需要重定向, url:", url, "。但是不对:", i)
                        return False, href
        else:
            return False, href

    # 去除带后缀的网址
    def pass_filename_extension(self, url):
        for i in self.filename_extension_upper_list:
            if url.endswith('.'+i):
                return False
        for i in self.filename_extension_lower_list:
            if url.endswith('.'+i):
                return False
        return True

    # 定义删除除字母,数字，汉字以外的所有符号的函数
    def remove_punctuation(self, line):
        line = str(line)
        if line.strip() == '':
            return ''
        r1 = u'[a-zA-Z0-9’!"#$%&\'()*+,-./:;<=>?@，。?★、…【】《》？“”‘’！[\\]^_`{|}~]+'
        r2 = u"[^a-zA-Z0-9\u4E00-\u9FA5]"
        rule = re.compile(u"[^<>]")
        line = rule.sub('', line)
        return line

    # 定义删除汉字以外的字符的函数
    def remove_punctuation_chinsese(self, line):
        line = str(line)
        if line.strip() == '':
            return ''
        r1 = u'[a-zA-Z0-9’!"#$%&\'()*+,-./:;<=>?@，。?★、…【】《》？“”‘’！[\\]^_`{|}~]+'
        r2 = u"[^\u4E00-\u9FA5]"
        rule = re.compile(r2)
        line = rule.sub('', line)
        return line

    # TODO 拼接新闻详情页的链接
    def url_pinjie(self, url, the_shortest_url):
        url = url.strip().replace(' ', '')
        if url.endswith("/"):
            url = url
        else:
            url = url + '/'

        # 拼接改进
        if (('product' in the_shortest_url) and ('product' in url)) or (('html' in the_shortest_url) and ('html' in url)):
            url_list_linshi = url.split('/')
            # print(url_list_linshi)
            url = '/'.join(url_list_linshi[:-1])
            # print(url)

        the_shortest_url = the_shortest_url
        url_join = urljoin(url, the_shortest_url)
        # print('url_join:', url_join)
        arr = parse.urlparse(url_join)
        # print('arr:', arr)
        # print('arr_netloc', arr.netloc)
        path = normpath(arr[2])
        # print('path:', path)
        result = urlunparse((arr.scheme, arr.netloc, path, arr.params, arr.query, arr.fragment))
        # result = url_join

        result = result.strip()
        if result.endswith(".html"):
            result = result
        else:
            if result.endswith("/"):
                result = result[:-1]
            else:
                result = result
        result = result.strip().replace(' ', '')
        return result

    # 自动编码判断
    def judge_charset(self, charset_judge_url):
        TestData = self.openlink(charset_judge_url)
        if TestData:
            bianma = chardet.detect(TestData)
            # print("编码-----------: {} \t detail_url: {} \t ".format(bianma, charset_judge_url))
            # print(bianma['encoding'])
            result_bianma = bianma['encoding']
            return result_bianma
        else:
            result_bianma = 'utf-8'
            return result_bianma

    # urllib timeout次数
    def openlink(self, charset_judge_url):
        maxTryNum = 5
        for tries in range(maxTryNum):
            try:
                TestData = urllib.request.urlopen(charset_judge_url).read()
                return TestData
            except:
                if tries < (maxTryNum - 1):
                    continue
                else:
                    self.logger.info("Has tried %d times to access url %s, all failed!", maxTryNum, charset_judge_url)
                    break

    def updateFile(self, file,old_str,new_str):
        """
        替换文件中的字符串
        :param file:文件名
        :param old_str:就字符串
        :param new_str:新字符串
        :return:
        """
        file_data = ""
        with open(file, "r", encoding="utf-8") as f:
            for line in f:
                if old_str in line:
                    line = line.replace(old_str,new_str)
                file_data += line
        with open(file,"w",encoding="utf-8") as f:
            f.write(file_data)


if __name__ == '__main__':
    a = Path(__file__).name
    print(a)
    num = a.replace('auto_product_spider', '').replace('.py', '')
    bp = ListDetailSpider(SAVE_MONGO_CONFIG, None, num)
    bp.run(
        ".../autoproduct_crawler_and_cleaning/ai_company_20200605_revise/ai_20200605_revise_" + num + ".txt",
        "1")
