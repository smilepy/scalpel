#!/usr/bin/python
#coding=utf-8

import MySQLdb
import sys
# import time
# import ConfigParser

reload(sys)
sys.setdefaultencoding('utf-8')


def generate_parentintent():
    # conf = ConfigParser.ConfigParser()
    # conf.read('./conf/dbconf.ini')  # 文件路径
    # host = conf.get("db", "host")
    # user=conf.get("db","user")
    # password=conf.get("db","password")
    # print("host:%s,user:%s,password:%s"%(host,user,password))
    try:
        # conn = MySQLdb.connect(host = 'ros-user.mysql.rds.aliyuncs.com', user = '******', passwd = '******', db = 'bot_bot', port = 3306, charset = 'utf8')
        conn = MySQLdb.connect(host = '192.168.1.31', user = 'root', passwd = '', db = 'bot_bot', port = 3306, charset = 'utf8')
    except:
        print 'cannot connect database bot_bot'
        exit(1)

    cur = conn.cursor()

    id_start=1L
    limits=10
    loop=0
    while True:
        loop+=1
        print("loop:%d,id_start:%d"%(loop,id_start))
        # sql="SELECT id,agentid,domainid,intentname FROM intent WHERE id>={0}  and domainid in (1168,1170)  LIMIT {1}".format(id_start,limits)
        sql = "SELECT id,agentid,domainid,intentname FROM intent WHERE id>={0} and (pid=0 or pid=NULL )  LIMIT {1}".format(
            id_start, limits)
        count=cur.execute(sql)
        if count>0:
            print("intent_count:%d" % count)
            intents = cur.fetchmany(count)
            for intent in intents:
                intent_id = intent[0]

                parentintent_id=0
                #判断父意图是否存在
                parent_sql="SELECT id,agentid,domainid,intentname FROM parentintent WHERE agentid={0} and domainid={1} and intentname='{2}' ".format(
                    intent[1], intent[2], intent[3])
                parent_count=cur.execute(parent_sql)
                if parent_count==0:
                    cur.execute("INSERT INTO parentintent(agentid,domainid,intentname) VALUES(%s,%s,%s) ",
                                (intent[1], intent[2], intent[3]))
                    parentintent_id = long(cur.lastrowid)

                else:
                    parentintents = cur.fetchmany(1)
                    for parentintent in parentintents:
                        parentintent_id=parentintent[0]

                intent_sql = "UPDATE intent SET pid={0} where id={1}".format(parentintent_id, intent_id)
                cur.execute(intent_sql)

                # print("intent_id:%d"%intent_id)
                # sql = "INSERT INTO parentintent(agentid,domainid,intentname) VALUES(%d,%d,%s) ".format(intent[1], intent[2], intent[3])

            id_start=intents[count-1][0]+1
        else :
            break

        print("//=====================================================================================================")
        # print("sleep 3 s")
        # time.sleep(3)

    cur.close()
    conn.commit()
    conn.close()




if __name__ == '__main__':
    generate_parentintent()
    print("finished generating parentintents!")