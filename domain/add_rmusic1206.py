#!/usr/bin/python
#coding=utf-8

import MySQLdb
import sys
import time

reload(sys)
sys.setdefaultencoding('utf-8')


def add_rmusic1206():
    #the rmusic domain
    #online
    #rmusic_domainid=1196
    #offline
    rmusic_domainid = 1146
    try:
        #online
        # conn_bot_store = MySQLdb.connect(host = 'ros-user.mysql.rds.aliyuncs.com', user = '******', passwd = '******', db = 'bot_store', port = 3306, charset = 'utf8')
        #offline
        conn_bot_store = MySQLdb.connect(host='192.168.1.31', user='root', passwd='', db='bot_store', port=3306,charset='utf8')
    except:
        print 'cannot connect database bot_store'
        exit(1)

    cur_bot_store=conn_bot_store.cursor()

    sql = "SELECT releasedid FROM domaininstore WHERE agentid=1 AND  domainid={0}".format(rmusic_domainid)
    count = cur_bot_store.execute(sql)

    releasedid=0
    if count > 0:
        domaininstores = cur_bot_store.fetchmany(count)
        for domaininstore in domaininstores:
            releasedid = domaininstore[0]
    else:
        print 'not find instore domain rmusic1206'
        exit(1)


    sql = "SELECT domainid,agentid,mllever,pred FROM domainreleased WHERE id={0} ".format(releasedid)
    count = cur_bot_store.execute(sql)

    domainid=0
    isfromsystem=False
    mllever=0
    pred=0
    if count > 0:
        domainreleaseds = cur_bot_store.fetchmany(count)
        for domainreleased in domainreleaseds:
            domainid = domainreleased[0]
            agentid = domainreleased[1]
            if agentid==1:
                isfromsystem=True
            mllever=domainreleased[2]
            pred=domainreleased[3]
    else:
        print 'not find released domain rmusic1206'
        exit(1)

    sql = "SELECT DISTINCT agentid FROM domainenabled "
    count = cur_bot_store.execute(sql)
    if count > 0:
        print("app_count:%d" % count)
        agents = cur_bot_store.fetchmany(count)
        for agent in agents:
            cur_agentid=agent[0]
            sql = "SELECT enabledomain FROM domainenabled WHERE agentid={0} and domainid in (912,994,995) ".format(cur_agentid)
            count = cur_bot_store.execute(sql)
            if count>0:
                listitem = cur_bot_store.fetchmany(count)
                final_enable=False
                for item in listitem:
                    if item[0]==True:
                        final_enable=True
                        break

                cur_time = int(time.time())
                cur_bot_store.execute(
                    "INSERT INTO domainenabled(agentid,releasedid,domainid,isfromsystem,enabledomain,updatetm,mllever,pred) VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE enabledomain=%s,updatetm=%s",
                    (cur_agentid, releasedid, domainid, isfromsystem, final_enable, cur_time, mllever, pred,
                     final_enable, cur_time))
            else:
                cur_bot_store.execute("DELETE FROM domainenabled WHERE agentid=%s AND domainid=%s",(cur_agentid,rmusic_domainid))
    else:
        print("not find app")

    cur_bot_store.close()
    conn_bot_store.commit()
    conn_bot_store.close()




if __name__ == '__main__':
    print("starttime:%s"% time.asctime( time.localtime(time.time())))
    add_rmusic1206()
    print("endtime:%s" % time.asctime(time.localtime(time.time())))
    print("finished add rmusic1206!")
    print("//=====================================================================================================")