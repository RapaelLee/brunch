#!/usr/bin/python
# -*- coding: utf-8 -*-
import sqlite3
import tensorflow as tf
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def load_data():
    sql = """
select gap_1507 / sum_gap as ratio_1507
     , gap_1508 / sum_gap as ratio_1508
     , gap_1509 / sum_gap as ratio_1509
     , gap_1510 / sum_gap as ratio_1510
     , gap_1511 / sum_gap as ratio_1511
     , gap_1512 / sum_gap as ratio_1512
     , gap_1601 / sum_gap as ratio_1601
     , gap_1602 / sum_gap as ratio_1602
     , gap_1603 / sum_gap as ratio_1603
     , gap_1604 / sum_gap as ratio_1604
     , gap_1605 / sum_gap as ratio_1605
     , gap_1606 / sum_gap as ratio_1606
     , gap_1607 / sum_gap as ratio_1607
     , gap_1608 / sum_gap as ratio_1608
     , gap_1609 / sum_gap as ratio_1609
     , gap_1610 / sum_gap as ratio_1610
     , gap_1611 / sum_gap as ratio_1611
     , gap_1612 / sum_gap as ratio_1612
     , gap_1701 / sum_gap as ratio_1701
     , gap_1702 / sum_gap as ratio_1702
     , gap_1703 / sum_gap as ratio_1703
  from (
        select item_cd
             , item_name
             , abs(avg_price_201507 - avg_price_201506) as gap_1507
             , abs(avg_price_201508 - avg_price_201507) as gap_1508
             , abs(avg_price_201509 - avg_price_201508) as gap_1509
             , abs(avg_price_201510 - avg_price_201509) as gap_1510
             , abs(avg_price_201511 - avg_price_201510) as gap_1511
             , abs(avg_price_201512 - avg_price_201511) as gap_1512
             , abs(avg_price_201601 - avg_price_201512) as gap_1601
             , abs(avg_price_201602 - avg_price_201601) as gap_1602
             , abs(avg_price_201603 - avg_price_201602) as gap_1603
             , abs(avg_price_201604 - avg_price_201603) as gap_1604
             , abs(avg_price_201605 - avg_price_201604) as gap_1605
             , abs(avg_price_201606 - avg_price_201605) as gap_1606
             , abs(avg_price_201607 - avg_price_201606) as gap_1607
             , abs(avg_price_201608 - avg_price_201607) as gap_1608
             , abs(avg_price_201609 - avg_price_201608) as gap_1609
             , abs(avg_price_201610 - avg_price_201609) as gap_1610
             , abs(avg_price_201611 - avg_price_201610) as gap_1611
             , abs(avg_price_201612 - avg_price_201611) as gap_1612
             , abs(avg_price_201701 - avg_price_201612) as gap_1701
             , abs(avg_price_201702 - avg_price_201701) as gap_1702
             , abs(avg_price_201703 - avg_price_201702) as gap_1703

             , abs(avg_price_201507 - avg_price_201506)
             + abs(avg_price_201508 - avg_price_201507)
             + abs(avg_price_201509 - avg_price_201508)
             + abs(avg_price_201510 - avg_price_201509)
             + abs(avg_price_201511 - avg_price_201510)
             + abs(avg_price_201512 - avg_price_201511)
             + abs(avg_price_201601 - avg_price_201512)
             + abs(avg_price_201602 - avg_price_201601)
             + abs(avg_price_201603 - avg_price_201602)
             + abs(avg_price_201604 - avg_price_201603)
             + abs(avg_price_201605 - avg_price_201604)
             + abs(avg_price_201606 - avg_price_201605)
             + abs(avg_price_201607 - avg_price_201606)
             + abs(avg_price_201608 - avg_price_201607)
             + abs(avg_price_201609 - avg_price_201608)
             + abs(avg_price_201610 - avg_price_201609)
             + abs(avg_price_201611 - avg_price_201610)
             + abs(avg_price_201612 - avg_price_201611)
             + abs(avg_price_201701 - avg_price_201612)
             + abs(avg_price_201702 - avg_price_201701)
             + abs(avg_price_201703 - avg_price_201702) as sum_gap
          from (
                select item_cd
                     , item_name
                     , avg(case when substr(item_ymd,1,6) = '201506' then item_price else 0 end) as avg_price_201506
                     , avg(case when substr(item_ymd,1,6) = '201507' then item_price else 0 end) as avg_price_201507
                     , avg(case when substr(item_ymd,1,6) = '201508' then item_price else 0 end) as avg_price_201508
                     , avg(case when substr(item_ymd,1,6) = '201509' then item_price else 0 end) as avg_price_201509
                     , avg(case when substr(item_ymd,1,6) = '201510' then item_price else 0 end) as avg_price_201510
                     , avg(case when substr(item_ymd,1,6) = '201511' then item_price else 0 end) as avg_price_201511
                     , avg(case when substr(item_ymd,1,6) = '201512' then item_price else 0 end) as avg_price_201512
                     , avg(case when substr(item_ymd,1,6) = '201601' then item_price else 0 end) as avg_price_201601
                     , avg(case when substr(item_ymd,1,6) = '201602' then item_price else 0 end) as avg_price_201602
                     , avg(case when substr(item_ymd,1,6) = '201603' then item_price else 0 end) as avg_price_201603
                     , avg(case when substr(item_ymd,1,6) = '201604' then item_price else 0 end) as avg_price_201604
                     , avg(case when substr(item_ymd,1,6) = '201605' then item_price else 0 end) as avg_price_201605
                     , avg(case when substr(item_ymd,1,6) = '201606' then item_price else 0 end) as avg_price_201606
                     , avg(case when substr(item_ymd,1,6) = '201607' then item_price else 0 end) as avg_price_201607
                     , avg(case when substr(item_ymd,1,6) = '201608' then item_price else 0 end) as avg_price_201608
                     , avg(case when substr(item_ymd,1,6) = '201609' then item_price else 0 end) as avg_price_201609
                     , avg(case when substr(item_ymd,1,6) = '201610' then item_price else 0 end) as avg_price_201610
                     , avg(case when substr(item_ymd,1,6) = '201611' then item_price else 0 end) as avg_price_201611
                     , avg(case when substr(item_ymd,1,6) = '201612' then item_price else 0 end) as avg_price_201612
                     , avg(case when substr(item_ymd,1,6) = '201701' then item_price else 0 end) as avg_price_201701
                     , avg(case when substr(item_ymd,1,6) = '201702' then item_price else 0 end) as avg_price_201702
                     , avg(case when substr(item_ymd,1,6) = '201703' then item_price else 0 end) as avg_price_201703
                  from base_prices
                 group by item_cd
                        , item_name
                ) aa
        ) bb
    """
    curr.execute(sql)
    return curr.fetchall()

if __name__ == "__main__":
    """
        참조 사이트 - https://tensorflow.blog/3-텐서플로우-클러스터링-first-contact-with-tensorflow/
    """
    # 클러스터 갯수
    CLUSTER_COUNT = 3
    # 샘플 저장 위치
    SQLITE_FILE_NAME = '../data/ar_sample_trans.sqlite'
    # SQLite3 연결
    conn = sqlite3.connect(SQLITE_FILE_NAME)
    curr = conn.cursor()
    # 데이터 추출
    data_from_table = load_data()
    # 상수 텐서 생성
    tensor_data = tf.constant(data_from_table)
    # 텐서 차원 추가
    expanded_vectors = tf.expand_dims(tensor_data, 0)
    # 임의의 중심점 선언
    centroids = tf.Variable(tf.slice(tf.random_shuffle(tensor_data), [0, 0], [CLUSTER_COUNT, -1]))
    # 텐서 차원 추가
    expanded_centroids = tf.expand_dims(centroids, 1)
    # 유클리디안 제곱거리(Squared Euclidean Distance) 할당
    tensor_difference = tf.subtract(expanded_vectors, expanded_centroids)
    tensor_square = tf.square(tensor_difference)
    tensor_distances = tf.reduce_sum(tensor_square, 2)
    assignments = tf.argmin(tensor_distances, 0)
    # K-means clustering
    #   1. equal 함수를 사용하여 한 클러스터와 매칭되는(역주: 클러스터 번호는 변수 c 에 매핑) assignments 텐서의 요소에
    #     true 표시가 되는 불리언(boolean) 텐서(Dimension(2000))를 만듭니다.
    #   2. where 함수를 사용하여 파라메타로 받은 불리언 텐서에서 true 로 표시된 위치를 값으로 가지는 텐서(Dimension(1) x Dimension(2000))를 만듭니다.
    #   3. reshape 함수를 사용하여 c 클러스터에 속한 tensor_data 텐서의 포인트들의 인덱스로 구성된 텐서(Dimension(2000) x Dimension(1))를 만듭니다.
    #     (역주: reshape의 텐서의 크기를 지정하는 파라메타의 두번째 배열요소가 -1이라 앞단계에서 만든 텐서를 차원을 뒤집는 효과를 발휘하여 [Dimension(1), Dimension(None)] 텐서를 만듭니다)
    #   4. gather 함수를 사용하여 c 클러스터를 구성하는 포인트의 좌표를 모은 텐서(Dimension(1) x Dimension(2000))를 만듭니다.
    #     (역주: [Dimension(1), Dimension(None), Dimension(2)] 텐서를 만듭니다)
    #   5. reduce_mean 함수를 사용하여 c 클러스터에 속한 모든 포인트의 평균 값을 가진 텐서(Dimension(1) x Dimension(2))를 만듭니다.
    means = tf.concat([tf.reduce_mean(tf.gather(tensor_data, tf.reshape(tf.where(tf.equal(assignments, c)), [1, -1])), reduction_indices=[1]) for c in xrange(CLUSTER_COUNT)], 0)
    # 중심점 업데이트
    update_centroids = tf.assign(centroids, means)
    # 데이터 그래프를 실행시키기 전에 모든 변수를 초기화
    init_op = tf.global_variables_initializer()
    # 데이터 그래프 실행
    sess = tf.Session()
    sess.run(init_op)
    for step in xrange(100):
        _, centroid_values, assignment_values = sess.run([update_centroids, centroids, assignments])
    print(centroid_values)
    print(assignment_values)
    # 결과를 확인하기 위해 산점도 그래프 생성
    data = {"x": [], "y": [], "cluster": []}
    for i in xrange(len(assignment_values)):
        data["x"].append(data_from_table[i][0])
        data["y"].append(data_from_table[i][1])
        data["cluster"].append(assignment_values[i])
    df = pd.DataFrame(data)
    sns.lmplot("x", "y", data=df, fit_reg=False, size=6, hue="cluster", legend=False)
    plt.show()
