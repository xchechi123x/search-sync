import pymysql
import configparser

# 加载现有配置文件
config = configparser.ConfigParser()
config.read('config.ini')
query = {
    "list": "SELECT id,name,phone,email,industry,company,department,position,icon,auth,auth_time,auth_img,auth_msg,code,referee_code,info,work_experience,create_time, update_time FROM sys_user LIMIT %s , %s",
    "count": "SELECT count(*) AS count FROM sys_user"
}

db_config = {
    'host': config.get('db', 'host'),
    'user': config.get('db', 'user'),
    'passwd': config.get('db', 'passwd'),
    'db': config.get('db', 'database')
}

print(db_config)


def get_sys_user(rows=500, start=0):
    connect = pymysql.connect(host=db_config['host'], user=db_config['user'], passwd=db_config['passwd'], db=db_config['db'], charset="utf8",
                              cursorclass=pymysql.cursors.DictCursor)
    cursor = connect.cursor()
    cursor.execute(query['count'])
    print(cursor.fetchone()['count'])

    cursor.execute(query['list'], (start, rows))

    user_list = cursor.fetchall()
    for user in user_list:
        print(user)


if __name__ == "__main__":
    get_sys_user()
