import time
import os

from hook import Hook
from sender import Shoot

from datetime import datetime
import tset_module
from tset_module.config2 import constant_vars


def fetch_shares():

    url = constant_vars['main_url']
    length_to_select = constant_vars['selecting_length']
    url_fetcher_obj = tset_module.TsetCrawler()
    url_fetcher = url_fetcher_obj.fetch_urls(url, length_to_select)

    return url_fetcher


def fetch_data(urls):
    complete_url = []

    shoot = Shoot('qu1')
    hook = Hook('qu2')

    path = constant_vars["crawled_data_dir"]
    os.makedirs(path, exist_ok=True)

    data_fetcher_obj = tset_module.TsetCrawler()
    tmp = 0

    today_date = datetime.today().strftime('%Y-%m-%d')

    table_last_check_init(urls)

    for i in urls:
        hook = Hook('qu2')
        make_table_i(i)

        tmp = tmp + 1
        if tmp > 50:
            return 0
        data_fetched = data_fetcher_obj.fetch_data(i)
        trimmer_no = data_fetched.split(';')

        last_update_query = f'request$$$SELECT last_update FROM last_check WHERE share_id={i};'

        shoot.send(last_update_query)

        hook.start_shit()

        last_update = hook.body
 
        hook.terminate()

        for jj in trimmer_no:
            row = jj.split('@')
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


def table_last_check_init(urls):
    temp2 = 0
    shoot2 = Shoot('qu1')
    for ii in urls:
        temp2 = temp2 + 1
        column_for_table = f'order$$$INSERT IGNORE INTO last_check(share_id) values({ii});'
        shoot2.send(column_for_table)
    shoot2.terminate()
    print('initiating done ', temp2)


def make_table_i(i):
    shoot2 = Shoot('qu1')
    table_making_query = f'order$$$CREATE TABLE IF NOT EXISTS `{i}`(date DATE NOT NULL,max_price int unsigned,min_price int unsigned, total int unsigned,last_price int unsigned,first_price int unsigned, yesterday_price int unsigned, val bigint, volume bigint unsigned,number int unsigned);'

    shoot2.send(table_making_query)
    shoot2.terminate()


def runner():
    start_time = time.time()

    fetched_shares = fetch_shares()
    print('collecting URLs took:', time.time() - start_time)

    start_time = time.time()

    fetch_data(fetched_shares)
    print('crawling data took:', time.time() - start_time)


if __name__ == '__main__':
    runner()
