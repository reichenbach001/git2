import time
import os
import random

from hook import Hook
from sender import Shoot

from datetime import datetime
import tset_module
from config2 import constant_vars

import threading
import concurrent.futures

def fetch_shares():

    #url = constant_vars['main_url']
    #length_to_select = constant_vars['selecting_length']
    #url_fetcher_obj = tset_module.TsetCrawler()
    #url_fetcher = url_fetcher_obj.fetch_urls(url, length_to_select)
    #start_from=random.randint(1,len(url_fetcher)-25)
    #urls=url_fetcher[start_from:start_from+25]  
    urls=['3176699243034971', '12777578088653944', '37938861018065443',
     '23214828924506640', '55289848471625247', '318005355896147',
      '11093176038132310', '7457232989848872', '67652526762453420',
       '13255663384634443', '15917865009187760', '59866041653103343',
        '52792903131341205', '204092872752957', '66424163876658304',
         '12253574954132925', '43622578471330344', '71672399601682259',
          '64843936383937546', '20133434564923831', '49869693814643443',
           '10568944722570445', '55373808401388162', '71523986304961239',
            '42479862703183824']

    return urls


def fetch_data(urls):
    complete_url = []



    shoot = Shoot(constant_vars['qeue_to_db'])

    data_fetcher_obj = tset_module.TsetCrawler()

    today_date = datetime.today().strftime('%Y-%m-%d')

    last_update_dict={}
    last_update_dict=get_last_update(urls,shoot)

    

    print('============================================================')
    with concurrent.futures.ThreadPoolExecutor(max_workers = 10) as executor:
        futuree={executor.submit(get_and_save,i,today_date,last_update_dict,data_fetcher_obj): i for i in urls}
        for future in concurrent.futures.as_completed(futuree):
            print(future.result())






def table_last_check_init(urls):
    temp2 = 0

    shoot2 = Shoot(constant_vars['qeue_to_db'])

    for iterator in urls:

        make_table_i(iterator,shoot2)
        temp2 = temp2 + 1
        column_for_table = f'order$$$INSERT IGNORE INTO last_check(share_id) values({iterator});'
        shoot2.send(column_for_table)

    shoot2.send('commit$$$')
    shoot2.terminate()
    print('initiating done ', temp2)


def make_table_i(i,shoot):
    #shoot2 = Shoot(constant_vars['qeue_to_db'])
    table_making_query = f'order$$$CREATE TABLE IF NOT EXISTS `{i}`(date DATE NOT NULL,max_price int unsigned,min_price int unsigned, total int unsigned,last_price int unsigned,first_price int unsigned, yesterday_price int unsigned, val bigint, volume bigint unsigned,number int unsigned);'

    shoot.send(table_making_query)
    #shoot2.terminate()

def hook_for_database_init():

    hook_db=Hook(constant_vars['qeue_to_db'])
    hook_db.start()

def get_last_update(urls,shoot):
    last_update={}
    for iterator in urls:
        hook=Hook(constant_vars['qeue_to_crawler'])
        last_update_query = f'request$$$SELECT last_update FROM last_check WHERE share_id={iterator};'
        shoot.send(last_update_query)
        hook.start()
        last_update[iterator] = hook.body
        hook.terminate_channel()
    print(last_update)

    return(last_update)




def get_and_save(i,today_date,last_update,data_fetcher_obj) :

    shoot = Shoot(constant_vars['qeue_to_db'])



    last_update_i=last_update[i]
    try:
        data_fetched = data_fetcher_obj.fetch_data(i)
    except Exception as e:
        print(e,' for ',i)
        data_fetched=[]

    trimmer_no = data_fetched.split(';')

    for iterator in trimmer_no:

        row = iterator.split('@')
        timestamp = ''.join(
            (row[0][0:4], '-', row[0][4:6], '-', row[0][6:8]))

        if timestamp < last_update_i:

            last_check_update_query = f'order$$$UPDATE last_check SET last_update="{today_date}" where share_id={i}; '
            shoot.send(last_check_update_query)
            shoot.send('commit$$$')
            print(i,' : Done','Thread',threading.current_thread(),' = ',timestamp,'<',last_update_i)
            break

        row[0] = timestamp
        if len(row) == 10:
            query = f'order$$$insert into `{i}` values("{row[0]}",{row[1]},{row[2]},{row[3]},{row[4]},{row[5]},{row[6]},{row[7]},{row[8]},{row[9]});'
  
            shoot.send(query)
        else:
            print(f'order$$$insert into `{i}` values("{row[0]}",{row[1]},{row[2]},{row[3]},{row[4]},{row[5]},{row[6]},{row[7]},{row[8]},{row[9]});')
    shoot.terminate()
    log=''.join(())


    return log

    


def runner():
    start_time = time.time()

    fetched_shares = fetch_shares()


    print('collecting URLs took:', time.time() - start_time)

    start_time = time.time()




    table_last_check_init(fetched_shares)

    fetch_data(fetched_shares)
    print('crawling data took:', time.time() - start_time)
    os._exit(0)


if __name__ == '__main__':
    th=threading.Thread(target=hook_for_database_init)
    th.start()

    print('qu1 started')
    runner()

