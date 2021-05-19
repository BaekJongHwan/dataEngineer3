from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime

dag = DAG(
	dag_id = 'my_two_dag',
	start_date = datetime(2021,5,16),
	schedule_interval = '0 2 * * *')
###
#############
#Python code
#############

import psycopg2

# Redshift connection 함수
def get_Redshift_connection():
    host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = "baekjonghwan"
    redshift_pass = "Baekjonghwan!1"
    port = 5439
    dbname = "prod"
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(autocommit=True)
    return conn.cursor()

import requests

def extract(url):
    f = requests.get(url)
    return (f.text)

def transform(text):
    lines = text.split("\n")
    return lines

def load(lines):
    # BEGIN과 END를 사용해서 SQL 결과를 트랜잭션으로 만들어주는 것이 좋음
    # BEGIN;DELETE FROM (본인의스키마).name_gender;INSERT INTO TABLE VALUES ('KEEYONG', 'MALE');....;END;
    cur = get_Redshift_connection()

    try:
      cur.execute("BEGIN")
      lines.sort()
      for r in lines:
          if r != '':
              (name, gender) = r.split(",")
              if name != 'name' and gender != 'gender':
                print(name, "-", gender)
                sql = "DELETE FROM baekjonghwan.name_gender; INSERT INTO baekjonghwan.name_gender VALUES ('{n}', '{g}')".format(n=name, g=gender)
                print(sql)
                cur.execute(sql)
      cur.execute("END")
      cur.commit;
    except:
      cur.execute("ROLLBACK")
    finally:
      cur.close();

def go():
    link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
    data = extract(link)
    lines = transform(data)
    load(lines)

####################
# Dag Task Setting
####################

go = PythonOperator(
	task_id = 'Gender_ETL',
	#python_callable param points to the function you want to run 
	python_callable = go,
	#dag param points to the DAG that this task is a part of
	dag = dag)