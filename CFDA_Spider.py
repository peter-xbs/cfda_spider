# _*_ coding:utf-8 _*_

import os
import re
import sys
import time
import argparse
import pickle
import requests
from bs4 import BeautifulSoup as BS
from collections import defaultdict
from config import get_header
from config import get_sleep_time
from selenium import webdriver

# ----------网页抓包分析-基本配置部分-------------------
# 具体药品详情页面url公共部分
drug_url_base = 'http://app1.sfda.gov.cn/datasearch/face3/'
# 药品列表页面url公共部分
page_url_base = 'http://app1.sfda.gov.cn/datasearch/face3/search.jsp?tableId={}&bcId={}&tableName=TABLE{}&State=1&curstart='
# 进口药品
imp_page_url = page_url_base.format(36, '124356651564146415214424405468', 36)
imp_pages_num = 273
# 进口药品商品名
imp_b_page_url = page_url_base.format(60, '124356657303811869543019763051', 60)
imp_b_pages_num = 407
# 国产药品
dom_page_url = page_url_base.format(25, '124356560303886909015737447882', 25)
dom_pages_num = 11060
# 国产药品商品名
dom_b_page_url = page_url_base.format(32, '124356639813072873644420336632', 32)
dom_b_pages_num = 469
# 关于pages_num，需登录CFDA网站查询每个大表的总页数，配置到脚本内

# 已完成爬取的drug ID默认文件
cwd = os.path.dirname(os.path.realpath(__file__))
id_pickle = os.path.join(cwd, 'CFDA_Config/FinishedID.pickle')
choices_set = {'imp', 'imp_b', 'dom', 'dom_b'}

# -------------参数配置部分--------------------

parser = argparse.ArgumentParser(description='APPLICATION: Collect and Update the CFDA DATA', formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('-F', dest='pickle', help='Provide the drug id have been already collected from CFDA', default=id_pickle)
parser.add_argument('-C', dest='choices', help='Provide the table choices, include("imp", "imp_b", "dom", "dom_b")', required=True)
parser.add_argument('-O', dest='output', help='Provide the output updated file_name!', required=True)
args = parser.parse_args()

finished_id_pickle = args.pickle
choice = args.choices
output = args.output

if os.path.isfile(finished_id_pickle):
    with open('CFDA_Config/FinishedID.pickle', 'rb') as f:
        FinishedIdDict = pickle.load(f)
else:
    FinishedIdDict = defaultdict(set)

if choice == 'imp':
    drug_page_url = imp_page_url
    pages_num = imp_pages_num
elif choice == 'imp_b':
    drug_page_url = imp_b_page_url
    pages_num = imp_b_pages_num
elif choice == 'dom':
    drug_page_url = dom_page_url
    pages_num = dom_pages_num
elif choice == 'dom_b':
    drug_page_url = dom_b_page_url
    pages_num = dom_b_pages_num
else:
    print('ERROR: Please provide the right table name in ("imp", "imp_b", "dom", "dom_b")!')
    sys.exit()

# ---------------网页爬取及配置文件更新部分-----------------
print(choice)
print(len(FinishedIdDict[choice]))

# 按页遍历CFDA所有药品信息，并收集最新的全部药品url
fo = open(output, 'a', encoding='utf-8')
driver = webdriver.Chrome()


def get_url(i, url):
    driver.get(url)
    time.sleep(0.05)
    html = driver.page_source
    soup = BS(html, 'html5lib')
    items = soup.select('tr td a')
    for item in items[1:]:
        drug_url = re.findall('(content\.jsp\?tableId.*?Id=\d{1,8})', str(item))[0]
        drug_url = re.sub('amp;', '', drug_url)
        fo.write(str(i)+'\t'+drug_url+'\n')
        fo.flush()

for i in range(1, pages_num+1):
    url = drug_page_url+str(i)
    print(url)
    if i % 100 == 0:
        time.sleep(1)
    get_url(i, url)

driver.close()
fo.close()

# 检查哪些Url已经被处理过，仅保留未爬取的url，并存入文件
wd = os.path.dirname(output)
updated_id = os.path.join(wd, choice+'_updated_ids.txt')
updated_output = os.path.join(wd, choice+'_updated_drugs.txt')
updated_filtered_output = os.path.join(wd, choice+'_updated_filtered_drugs.txt')
fo = open(updated_id, 'w', encoding='utf-8')
with open(output, encoding='utf-8', errors='ignore') as f:
    for line in f:
        line_list = line.strip().split('\t')
        drug_id = re.split('&Id=', line_list[1])[1]
        if drug_id not in FinishedIdDict[choice]:
            fo.write(line)
            FinishedIdDict[choice].add(drug_id)
    fo.close()
with open(finished_id_pickle, 'wb') as fo:
    pickle.dump(FinishedIdDict, fo)

# 爬取收集新更新药品信息，并存入文件
def parse_page(html):
    soup = BS(html)
    records = soup.find_all(attrs={"width": "83%"})
    headers = soup.find_all(attrs={"width": "17%"})
    records_text = [item.text for item in records]
    headers_text = [item.text for item in headers]
    record = '\t'.join(records_text) + '\n'
    header = '\t'.join(headers_text) + '\n'
    return header, record


with open(updated_output, 'w', encoding='utf-8') as fo:
    with open(updated_id, encoding='utf-8') as f:
        browser = webdriver.Chrome()
        k = 0
        for line in f:
            k += 1
            url = drug_url_base + line.strip().split('\t')[1]
            browser.get(url)
            time.sleep(0.05)
            html = browser.page_source
            parser_list = parse_page(html)
            new_line = parser_list[1]
            if k % 100 == 0:
                time.sleep(1)
            if k == 1:
                header_line = parser_list[0]
                fo.write(header_line)
                fo.write(new_line)
            else:
                fo.write(new_line)

        browser.close()

# 由于仅靠url中的id来确定有些风险，此处再用批准文号来确定需更新的具体药品信息
def filter_updated_drugs():
    with open(updated_filtered_output, 'w', encoding='utf-8') as fo:
        with open(updated_output, encoding='utf-8') as f2:
            i = 0
            for line in f2:
                i += 1
                id_ = line.strip().split('\t')[0]
                if i == 1:
                    fo.write(line)
                if id_ not in id_set:
                    id_set.add(id_)
                    fo.write(line)

with open('CFDA_Config/PermitNum.pickle', 'rb') as f1:
    id_set = pickle.load(f1)
filter_updated_drugs()
with open('CFDA_Config/PermitNum.pickle', 'wb') as fo:
    pickle.dump(id_set, fo)