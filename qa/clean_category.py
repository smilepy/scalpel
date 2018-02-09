#!/usr/bin/python
#coding=utf-8

import MySQLdb
import sys
import time
import ConfigParser

reload(sys)
sys.setdefaultencoding('utf-8')


def clean_category():
    conf = ConfigParser.ConfigParser()
    conf.read('./conf/dbconf.ini')  # 文件路径
    host = conf.get("db", "host")
    user=conf.get("db","user")
    password=conf.get("db","password")
    print("host:%s,user:%s,password:%s"%(host,user,password))
    try:
        conn = MySQLdb.connect(host = host, user = user, passwd = password, db = 'bot_qa_user', port = 3306, charset = 'utf8')
    except:
        print 'cannot connect database bot_qa_user'
        exit(1)

    max_cid=0L
    cur = conn.cursor()

    count = cur.execute("SELECT id FROM category ORDER BY id DESC LIMIT 1")
    categorys = cur.fetchmany(count)
    for c in categorys:
        max_cid=c[0]
    max_cid+=100L
    print("max_cid:%d"%max_cid)
    page_index=1L
    page_size=1000L
    is_break=0

    while True:
        id_start=(page_index-1L)*page_size+1L
        id_end=page_index*page_size
        if id_start>max_cid:
            print("id_start:%d"%id_start)
            break
        if id_end>max_cid:
            id_end=max_cid
            is_break=1

        ids = []
        cids=[]
        for id in range(id_start,id_end+1L):
            ids.append(long(id))
        print(ids)
        sql="SELECT id FROM category WHERE id>={0} and id<={1}".format(id_start,id_end)
        count=cur.execute(sql)
        categorys=cur.fetchmany(count)
        for c in categorys:
            cids.append(c[0])
        print(cids)

        deleted_cids=list(set(ids).difference(set(cids)))
        print("deleted_cids:%s"%deleted_cids)

        #cascade delete operation
        for p_cid in deleted_cids:
            descendants=[]
            get_descendants_id(p_cid,descendants,cur)
            descendants.append(p_cid)
            print("descendants:%s"%descendants)
            for cid in descendants:
                try:
                    conn2 = MySQLdb.connect(host = host, user = user, passwd = password, db='bot_qa_user', port=3306,
                                           charset='utf8')
                    cur2 = conn2.cursor()

                    # delete category
                    sql = "DELETE FROM category WHERE id={0}".format(cid)
                    cur2.execute(sql)

                    # get groups of the current category
                    sql = "SELECT id FROM `group` WHERE categoryid={0}".format(cid)
                    count = cur2.execute(sql)
                    groups = cur2.fetchmany(count)
                    gids = []
                    for g in groups:
                        gids.append(g[0])
                    print("cid:%d,gids:%s" % (cid, gids))

                    for g_id in gids:
                        #delete group
                        sql="DELETE FROM `group`  WHERE id={0}".format(g_id)
                        cur2.execute(sql)

                        #delete the questions of the current group
                        sql="DELETE FROM question where groupid={0}".format(g_id)
                        cur2.execute(sql)

                        #delete the answers of the current group
                        sql = "DELETE FROM answer where groupid={0}".format(g_id)
                        cur2.execute(sql)
                except :
                    cur2.close()
                    conn2.rollback()
                else:
                    cur2.close()
                    conn2.commit()
            print("//===================================================")
        page_index+=1
        print("//=====================================================================================================")
        if is_break==1:
            break
        print("sleep 3 s")
        time.sleep(3)

    cur.close()
    conn.commit()
    conn.close()


def get_descendants_id(pid, descendants,cur):
    try:
        sql = "SELECT id FROM category WHERE pid={0} LIMIT 10000".format(pid)
        count = cur.execute(sql)
        print("count:%d"%count)
        categorys = cur.fetchmany(count)
        for c in categorys:
            descendants.append(c[0])
            get_descendants_id(c[0],descendants,cur)
    except:
        print('GetDescendants func error')
        exit(1)


if __name__ == '__main__':
    clean_category()
    print("finished cleaning category and related tables(group,question,answer)！")