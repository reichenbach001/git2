import time
import os

from hook import Hook
from sender import Shoot

from datetime import datetime
import tset_module
from config2 import constant_vars

from threading import Thread
import concurrent.futures
from time_measure import Measure

def fetch_shares():
    '''
    url = constant_vars['main_url']
    length_to_select = constant_vars['selecting_length']
    url_fetcher_obj = tset_module.TsetCrawler()
    url_fetcher = url_fetcher_obj.fetch_urls(url, length_to_select)
    #start_from=random.randint(1,len(url_fetcher)-250)
    #urls=url_fetcher[start_from:start_from+250]   
    urls=url_fetcher
    '''
    urls=['44546510149749681', '26780282166315918', '62603302940123327',
     '40411537531154482', '27299841173245405', '27654159921979876',
      '11372565594192822', '58441662689656407', '58602432837130018',
       '23891830829322971', '28809886765682162', '37113329142641973',
        '23061497667178737', '20453828618330936', '47484909397760341',
         '52402259970992754', '68639224778935656', '56488870487464149',
          '42190112624335550', '32357363984168442', '50689220001642146',
           '778253364357513', '61506294208022391', '16056283141617755',
            '53076981031757046']


    return urls


def fetch_data(urls,last_update_dict):

    data_fetcher_obj = tset_module.TsetCrawler()

    today_date = datetime.today().strftime('%Y-%m-%d')

    

    with concurrent.futures.ThreadPoolExecutor(max_workers = 10) as executor:
        futuree={executor.submit(get_and_save,i,today_date,last_update_dict,data_fetcher_obj): i for i in urls}
        for future in concurrent.futures.as_completed(futuree):
            print(future.result())
'''
    for i in urls:
        get_and_save(i,today_date,last_update_dict,data_fetcher_obj)
        print(i,'done')
'''





def table_last_check_init(urls):
    temp2 = 0

    shoot2 = Shoot(constant_vars['qeue_to_db'])

    for iterator in urls:

        temp2 = temp2 + 1
        column_for_table = f'order$$$INSERT IGNORE INTO last_check(share_id) values({iterator});'
        shoot2.send(column_for_table)

    shoot2.send('commit$$$')
    shoot2.terminate()
    #print('initiating done ', temp2)


def make_tables(urls):
    shoot = Shoot(constant_vars['qeue_to_db'])
    
    for i in urls:
        table_making_query = f'order$$$CREATE TABLE IF NOT EXISTS t{i}(date DATE NOT NULL,max_price int unsigned,min_price int unsigned, total int unsigned,last_price int unsigned,first_price int unsigned, yesterday_price int unsigned, val bigint, volume bigint unsigned,number int unsigned);'

        shoot.send(table_making_query)
    
    shoot.send('commit$$$')
    shoot.terminate()

def hook_for_database_init():

    hook_db=Hook(constant_vars['qeue_to_db'])
    hook_db.start()


def get_last_update(urls):

    last_update_dict={}
    shoot=Shoot(constant_vars['qeue_to_db'])
    for iterator in urls:
        
        hook=Hook(constant_vars['qeue_to_crawler'])
        last_update_query = f'request$$$SELECT last_update FROM last_check WHERE share_id={iterator};'
        shoot.send(last_update_query)
        
        hook.start()
        last_update_dict[iterator] = hook.body
        hook.terminate_channel()

    shoot.terminate()
    return(last_update_dict)




def get_and_save(i,today_date,last_update,data_fetcher_obj) :

    shoot = Shoot(constant_vars['qeue_to_db'])
    last_update_i=last_update[i]
    
    try:
        data_fetched = data_fetcher_obj.fetch_data(i)
        trimmer_no = data_fetched.split(';')
        trimmer_no=trimmer_no[:len(trimmer_no)-1]
    except Exception as e:
        print(e,' for ',i)
        trimmer_no=[]



    for iterator in trimmer_no:

        row = iterator.split('@')

        timestamp = ''.join(
            (row[0][0:4], '-', row[0][4:6], '-', row[0][6:8]))

        if check_duplicate_write(timestamp,last_update_i,today_date,i):
            break

        row[0] = timestamp

        if len(row) == 10:
            query = f'order$$$insert into t{i} values("{row[0]}",{row[1]},{row[2]},{row[3]},{row[4]},{row[5]},{row[6]},{row[7]},{row[8]},{row[9]});'

            shoot.send(query)

    last_check_update_query = f'order$$$UPDATE last_check SET last_update="{today_date}" where share_id={i}; '
    shoot.send(last_check_update_query)
    shoot.send('commit$$$')
    shoot.terminate()

    log=''.join(('[*]done: ',str(i)))


    return log

    

def check_duplicate_write(timestamp,last_update_i,today_date,i):
    if timestamp < last_update_i:
        shoot = Shoot(constant_vars['qeue_to_db'])

        last_check_update_query = f'order$$$UPDATE last_check SET last_update="{today_date}" where share_id={i}; '
        shoot.send(last_check_update_query)

        shoot.send('commit$$$')
        shoot.terminate()
        return True
    return False

def qeue_empty_check():
    status=Hook(constant_vars['qeue_to_db'])
    
    while True:
        state=status.channel.queue_declare(constant_vars['qeue_to_db'],passive=True)
        if state.method.message_count==0:
            return True
        time.sleep(5)


def runner():
    measure=Measure()

    th=Thread(target=hook_for_database_init)
    th.start()
    
    measure.start()
    fetched_shares = fetch_shares()
    print('...collecting URLs took:', measure.stop())

    
    make_tables(fetched_shares) 
    table_last_check_init(fetched_shares)
    last_update_for_urls=get_last_update(fetched_shares)
    print('...initiating tables and getting last update took:',measure.stop())
    
    fetch_data(fetched_shares,last_update_for_urls)

    print('...crawling data took:',measure.stop())

    if qeue_empty_check():
        print('...emptying RabbitMQ took:',measure.stop())
        os._exit(0)

if __name__ == '__main__':
    runner()

