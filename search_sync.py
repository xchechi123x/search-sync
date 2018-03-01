import configparser
import json
import logging
import time
from datetime import timedelta, datetime

import daemon
import pymysql
from apscheduler.schedulers.blocking import BlockingScheduler
from daemon import pidfile
from elasticsearch import Elasticsearch

logger = logging.getLogger()
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler("sync.log", encoding="utf8")
logger.addHandler(file_handler)

query = {
    "list": "SELECT id,name,phone,email,industry,company,department,position,icon,auth,auth_time,auth_img,auth_msg,code,referee_code,info,work_experience,create_time, update_time FROM sys_user WHERE auth = 3 LIMIT %s , %s",
    "count": "SELECT count(*) AS count FROM sys_user WHERE auth = 3"
}

config = configparser.ConfigParser()
config.read('config.ini')

db_config = {
    'host': config.get('db', 'host'),
    'user': config.get('db', 'user'),
    'passwd': config.get('db', 'passwd'),
    'db': config.get('db', 'database')
}


def get_sys_user(rows=500, start=0):
    connect = pymysql.connect(host=db_config['host'], user=db_config['user'], passwd=db_config['passwd'],
                              db=db_config['db'], charset="utf8",
                              cursorclass=pymysql.cursors.DictCursor)
    cursor = connect.cursor()
    cursor.execute(query['count'])
    max_start = cursor.fetchone()['count']
    while True:
        cursor.execute(query['list'], (start, rows))
        if cursor.rowcount > 0:
            user_list = cursor.fetchall()
            yield user_list
            start = start + rows
        if cursor.rowcount < rows or start > max_start:
            cursor.close()
            break


def get_doc_user(user):
    work_experience = []
    if user['work_experience'] != '':
        work_experience = json.loads(user['work_experience'])
    doc_body = {
        "id": user["id"],
        "phone": user["phone"],
        "name": user["name"],
        "industry": user["industry"],
        "company": user["company"],
        "department": user["department"],
        "position": user["position"],
        "icon": user["icon"],
        "auth": user["auth"],
        "authImg": user["auth_img"],
        "email": user["email"],
        "authTime": user["auth_time"].strftime("%Y-%m-%d %H:%M:%S"),
        "authMsg": user["auth_msg"],
        "code": user["code"],
        "refereeCode": user["referee_code"],
        "info": user["info"],
        "workExperience": work_experience,
        "createTime": user["create_time"].strftime("%Y-%m-%d %H:%M:%S"),
        "updateTime": user["update_time"].strftime("%Y-%m-%d %H:%M:%S")
    }
    logger.info('get-db-user->' + str(user["id"]))
    return doc_body


elasticsearch = {
    'host': config.get('elasticsearch', 'host'),
    'index_prefix': config.get('elasticsearch', 'index_prefix'),
}


def search_sync():
    logger.info('task-start-time:' + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    es = Elasticsearch([elasticsearch['host']])
    user_list = get_sys_user(rows=1, start=0)
    index_name = elasticsearch['index_prefix'] + time.strftime('%Y.%m.%d', time.localtime(time.time()))
    for user in user_list:
        if len(user) > 0:
            doc_user = get_doc_user(user[0])
            logger.debug(doc_user)
            es.index(index=index_name, doc_type="user", refresh=True, body=doc_user)
            logger.info('index-user->' + str(doc_user['id']))
    yesterday_index_name = 'jinke-user-' + (datetime.today() + timedelta(-1)).strftime('%Y.%m.%d')
    logger.info('delete-index->' + yesterday_index_name)
    es.indices.delete(index=yesterday_index_name, ignore=[400, 404])


def start_scheduler():
    scheduler = BlockingScheduler()
    scheduler.add_job(search_sync, 'cron',
                      hour=config.getint('task-time', 'hour'),
                      minute=config.getint('task-time', 'minute'),
                      id='search_sync')
    scheduler.start()


pid = config.get('system', 'pid')
if __name__ == "__main__":
    with daemon.DaemonContext(files_preserve=[file_handler.stream],
                              pidfile=pidfile.TimeoutPIDLockFile(pid)):
        start_scheduler()
