from asyncio import futures
from logging import exception
import time
import os
import random
import concurrent.futures
from hook import Hook
from sender import Shoot

from datetime import datetime
import tset_module
from config2 import constant_vars

import threading

def fetch_shares():

    url = constant_vars['main_url']
    length_to_select = constant_vars['selecting_length']
    url_fetcher_obj = tset_module.TsetCrawler()
    url_fetcher = url_fetcher_obj.fetch_urls(url, length_to_select)

    return url_fetcher


def fetch_data(urls):
    complete_url = []

    th=threading.Thread(target=hook_for_database_init)
    th.start()

    shoot = Shoot(constant_vars['qeue_to_db'])

    data_fetcher_obj = tset_module.TsetCrawler()
    tmp = 0

    today_date = datetime.today().strftime('%Y-%m-%d')

    table_last_check_init(urls)
    # to limit crawling to 25
    start_from=random.randint(1,len(urls)-25)
    urls=urls[start_from:start_from+250]   
    # limiting done 
    #  
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        futures={executor.submit(get_and_save,i,today_date,data_fetcher_obj,shoot): i for i in urls}





def table_last_check_init(urls):
    temp2 = 0

    shoot2 = Shoot(constant_vars['qeue_to_db'])

    for iterator in urls:
        temp2 = temp2 + 1
        column_for_table = f'order$$$INSERT IGNORE INTO last_check(share_id) values({iterator});'
        shoot2.send(column_for_table)

    shoot2.terminate()
    print('initiating done ', temp2)


def make_table_i(i):
    shoot2 = Shoot(constant_vars['qeue_to_db'])
    table_making_query = f'order$$$CREATE TABLE IF NOT EXISTS `{i}`(date DATE NOT NULL,max_price int unsigned,min_price int unsigned, total int unsigned,last_price int unsigned,first_price int unsigned, yesterday_price int unsigned, val bigint, volume bigint unsigned,number int unsigned);'

    shoot2.send(table_making_query)
    shoot2.terminate()

def hook_for_database_init():

    hook_db=Hook(constant_vars['qeue_to_db'])
    hook_db.start()

def get_and_save(i,today_date,data_fetcher_obj,shoot) :

    hook=Hook(constant_vars['qeue_to_crawler'])
    
    #try:
    make_table_i(i)
    
    data_fetched = data_fetcher_obj.fetch_data(i)
    trimmer_no = data_fetched.split(';')

    last_update_query = f'request$$$SELECT last_update,share_id FROM last_check WHERE share_id={i};'
    shoot.send(last_update_query)

    hook.start()
    last_update_tmp= hook.body

    last_update=last_update_tmp.split(',')[0]
    print(i,' : ',last_update_tmp.split(',')[1])

    hook.terminate_channel()

    for iterator in trimmer_no:

        row = iterator.split('@')
        timestamp = ''.join(
            (row[0][0:4], '-', row[0][4:6], '-', row[0][6:8]))

        if timestamp < last_update:
            last_check_update_query = f'order$$$UPDATE last_check SET last_update="{today_date}" where share_id={i}; '
            shoot.send(last_check_update_query)
            shoot.send('commit$$$')

            break

        row[0] = timestamp
        if len(row) == 10:
            query = f'order$$$insert into `{i}` values("{row[0]}",{row[1]},{row[2]},{row[3]},{row[4]},{row[5]},{row[6]},{row[7]},{row[8]},{row[9]});'
            shoot.send(query)
    #except Exception as e:
       # print('url:',i,'raises error:',e)

    


def runner():
    start_time = time.time()

    try:
        fetched_shares = fetch_shares()
    except Exception as e:
        print(e)
        print('sleeping for 5 secs')
        time.sleep(5)
        fetched_shares = fetch_shares()



    print('collecting URLs took:', time.time() - start_time)

    start_time = time.time()

    fetch_data(fetched_shares)
    print('crawling data took:', time.time() - start_time)
    os._exit(0)


if __name__ == '__main__':
    runner()

