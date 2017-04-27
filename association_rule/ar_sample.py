#!/usr/bin/python
# -*- coding: utf-8 -*-
import math
import random
import sqlite3


# 난수를 레일리 분포로 만드는 함수
def inv_rayleigh_cdf(u):
    return math.sqrt(-2 * math.log(1 - u))


# 테이블 생성
def create_tables():
    try:
        curr.execute("drop table base_customer")
        conn.commit()
    except:
        pass
    curr.execute("""
    create table base_customer (
    customer_no         int,
    customer_id         text,
    customer_nm         text
    )
    """)
    conn.commit()
    try:
        curr.execute("drop table base_prices")
        conn.commit()
    except:
        pass
    curr.execute("""
    create table base_prices (
    item_no             int,
    item_cd             text,
    item_name           text,
    item_ymd            text,
    item_price          int,
    item_grp            text,
    item_wics           text,
    item_market         text
    )
    """)
    conn.commit()
    try:
        curr.execute("drop table base_purchase")
        conn.commit()
    except:
        pass
    curr.execute("""
    create table base_purchase (
    customer_no         int,
    item_no             int
    )
    """)
    conn.commit()
    try:
        curr.execute("drop table ar_samples")
        conn.commit()
    except:
        pass
    curr.execute("""
    create table ar_samples (
    customer_id         text,
    item_ymd            text,
    item_cd             text,
    item_grp            text,
    item_wics           text,
    item_market         text
    )
    """)
    conn.commit()


# 샘플로부터 테이블에 적재
def load_data():
    with open('ar_sample_customer.txt', 'r') as customer_file:
        lines = customer_file.readlines()
        for line in lines:
            cells = line.split('|')
            sql = ("""
            insert into base_customer
            (customer_no,customer_id,customer_nm)
            values (
              '""" + str(cells[0]) + """'
            , '""" + str(cells[1]) + """'
            , '""" + str(cells[2]).replace('\n', '') + """'
            )
            """)
            # print(sql)
            curr.execute(sql)
        conn.commit()
    curr.execute("select count(*) as cnt from base_customer")
    # print(str(curr.fetchone()[0]) + ' is loaded. (CUSTOMER)')
    with open('ar_sample_prices.txt', 'r') as customer_file:
        lines = customer_file.readlines()
        for line in lines:
            cells = line.split('|')
            curr.execute("""
            insert into base_prices values (
              '""" + str(cells[0]) + """'
            , '""" + str(cells[1]) + """'
            , '""" + str(cells[2]) + """'
            , """ + str(cells[3]) + """
            , '""" + str(cells[4]) + """'
            , '""" + str(cells[5]) + """'
            , '""" + str(cells[6]) + """'
            , '""" + str(cells[7]) + """'
            )
            """)
        conn.commit()
    curr.execute("select count(*) as cnt from base_prices")
    # print(str(curr.fetchone()[0]) + ' is loaded. (PRICES)')


# 난수 값 생성
def build_sample(p_sample_count):
    curr.execute("select count(*) as cnt from base_customer")
    count_of_customer = curr.fetchone()[0]
    random_numbers = [random.random() for _ in range(0, p_sample_count)]
    random_customer = [inv_rayleigh_cdf(random_number) for random_number in random_numbers]
    max_rand_customer_number = max(random_customer)
    # print(len(random_customer), min(random_customer), max(random_customer))
    curr.execute("select count(*) as cnt from base_prices")
    count_of_prices = curr.fetchone()[0]
    random_numbers = [random.random() for _ in range(0, p_sample_count)]
    random_prices = [inv_rayleigh_cdf(random_number) for random_number in random_numbers]
    max_rand_item_num = max(random_prices)
    # print(len(random_prices), min(random_prices), max(random_prices))
    for i in range(0, p_sample_count):
        sql = """
        insert into base_purchase
        (customer_no,item_no)
        values (
          '""" + str(round(random_customer[i] / max_rand_customer_number * count_of_customer)) + """'
        , '""" + str(round(random_prices[i] / max_rand_item_num * count_of_prices)) + """'
        )
        """
        curr.execute(sql)
    conn.commit()
    sql = ("""
    insert into ar_samples
    select t2.customer_id
         , t3.item_ymd
         , t3.item_cd
         , t3.item_grp
         , t3.item_wics
         , t3.item_market
      from base_purchase        t1
     inner join base_customer   t2
             on t1.customer_no = t2.customer_no
     inner join base_prices     t3
             on t1.item_no = t3.item_no
     order by t2.customer_id
            , t3.item_ymd
            , t3.item_cd
    """)
    curr.execute(sql)
    conn.commit()

if __name__ == "__main__":
    # 샘플 갯수
    SAMPLE_COUNT = 400000
    # 샘플 저장 위치
    SQLITE_FILE_NAME = '../data/ar_sample_trans.sqlite'
    conn = sqlite3.connect(SQLITE_FILE_NAME)
    curr = conn.cursor()

    # 샘플 생성 및 저장
    create_tables()
    load_data()
    build_sample(SAMPLE_COUNT)

    '''
    # R과 연동하여 R의 함수 수행 후 결과 표시
    import os
    import rpy2.robjects as robjects

    # print(os.getenv('R_HOME'))
    robjects.r("""source('ar_sample.R')""")
    r_function = robjects.globalenv['run_ar_sample']
    r_result = r_function(SQLITE_FILE_NAME, os.path.dirname(os.path.abspath(__file__)))

    import numpy as np
    r_data = np.array(r_result)
    r_data_array = []
    for row in r_data:
        r_data_array.append(row)
    length = len(r_data_array[0])

    ar_result = []
    for idx in range(0, length):
        ar_result.append({
            'left_had_side': r_data_array[0][idx],
            'right_hand_side': r_data_array[2][idx],
            'support': r_data_array[3][idx],
            'confidence': r_data_array[4][idx],
            'lift': r_data_array[5][idx]
        })

    for row in ar_result:
        print(row)
    '''
