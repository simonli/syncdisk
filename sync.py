# -*- coding:utf-8 -*-
from __future__ import with_statement
import time
import os
import sqlite3
import shutil
import subprocess
import psycopg2
import datetime
import sys

username=os.environ.get("username").lower()

src_dir = u"D:"+os.sep+username
dst_dir = u"\\\\"+"sync_server"+os.sep+username+"_bak"# 目标文件夹

config_dir = src_dir+os.sep+".xlive"
db_path = config_dir+os.sep+"xlive.db"

DIR_EXCLUDES = [src_dir+os.sep+"$RECYCLE.BIN",
                config_dir,
                ]

conn = None


def get_conn():
    """
    获取远程中央控管数据库连接
    """
    try:
        conn = psycopg2.connect(host="10.6.13.131",user="auosync",password="!auosync!",database="psgdb1")   
        return conn
    except Exception:
        return None
    
def get_remote_server(conn):
    remote_server = ""
    c = conn.cursor()
    c.execute("select remote_server from sync_meta where username=%s",(username,))
    line = c.fetchone()
    if line[0]:
        remote_server = line[0]
    return remote_server

def get_cx():
    cx = sqlite3.connect(db_path)
    text_factory = sqlite3.OptimizedUnicode
    return cx

def init_check():
    flag = False
    if not os.path.exists(src_dir):
        print "The source directory does not exist."
    else:
        flag = True
    return flag
    
def init_config():    
    if not os.path.exists(config_dir):
        os.mkdir(config_dir)
        subprocess.call("attrib +h +s %s" % config_dir,shell=True)        
    if not os.path.exists(db_path):
        cx = get_cx()
        c = cx.cursor()
        #创建table
        c.execute("""create table src_tree(is_dir text,file_path text,virtual_path text,last_mod_time float,file_size float,local_time datetime)""")
        c.execute("""create table snapshot(is_dir text,file_path text,virtual_path text,last_mod_time float,file_size float,local_time datetime)""")
        c.execute("""create table sync_log(is_dir text,file_path text,virtual_path text,last_mod_time float,file_size float,task_type VARCHAR(260),local_time datetime)""")
        c.execute("""create table sync_meta(k text, v text)""")
        #创建INDEX
        c.execute("""CREATE INDEX src_tree_index_01  ON src_tree (is_dir ASC,file_path ASC,virtual_path ASC, last_mod_time ASC,file_size ASC,local_time ASC)""")
        c.execute("""CREATE INDEX snapshot_index_01  ON snapshot (is_dir ASC,file_path ASC,virtual_path ASC, last_mod_time ASC,file_size ASC,local_time ASC)""")
        c.execute("""CREATE INDEX sync_log_index_01  ON sync_log (is_dir ASC,file_path ASC,virtual_path ASC, last_mod_time ASC,file_size ASC,task_type ASC,local_time ASC)""")
        c.execute("""CREATE INDEX sync_meta_index_01  ON sync_meta(k ASC, v ASC)""")
        #初始化数据
        c.execute("insert into sync_meta(k,v) values (?,?)",("sync_success_file_count",0))
        c.execute("insert into sync_meta(k,v) values (?,?)",("sync_success_file_size",0))
        c.execute("insert into sync_meta(k,v) values (?,?)",("sync_success_dir_count",0))
        c.execute("insert into sync_meta(k,v) values (?,?)",("last_sync_time","2012-05-01 00:00:00"))
        cx.commit()
        cx.close()
        
def cleart_db(cx):
    c = cx.cursor()
    c.execute("delete from src_tree")
    c.execute("delete from sync_meta where k='scan_file_count'")
    c.execute("delete from sync_meta where k='scan_dir_count'")
    c.execute("delete from sync_meta where k='scan_file_size'")
    cx.commit()

def scan_src_tree(cx):    
    dir_list = []
    file_list = []
    total_file_size = 0    
    time_now = datetime.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M:%S")
    for root,dirs,files in os.walk(src_dir):        
        for xfile in files: #获得所有文件
            file_path = os.path.join(root,xfile)
            if os.path.dirname(file_path) not in DIR_EXCLUDES:            
                file_attr = os.stat(file_path)
                virtual_path = file_path.rpartition(src_dir+os.sep)[2]                
                file_modify_time = round(file_attr.st_mtime, 1) # 精确度为0.1秒                
                file_size = round(file_attr.st_size,1) #精确到0.1KB            
                total_file_size = total_file_size + file_size             
                file_list.append(("N",file_path,virtual_path,file_modify_time,file_size,time_now))
               
        for xdir in dirs: #获得所有目录
            dir_path = os.path.join(root,xdir)
            if dir_path not in DIR_EXCLUDES:
                virtual_path = dir_path.rpartition(src_dir+os.sep)[2]
                dir_modify_time = round(os.stat(dir_path).st_mtime, 1) # 精确度为0.1秒   
                dir_list.append(("Y",dir_path,virtual_path,dir_modify_time,0,time_now))
    
          
    file_count = len(file_list) #文件数
    dir_count = len(dir_list) #目录数
    
    if file_count: #存入文件列表到数据库  if file_count > 0 
        c = cx.cursor()
        sql = """insert into src_tree(is_dir,file_path,virtual_path,last_mod_time,file_size,local_time) values (?,?,?,?,?,?)"""
        c.executemany(sql,file_list)
        #更新扫描到的文件数
        c.execute("insert into sync_meta(k,v) values (?,?)",("scan_file_count",file_count))
        c.execute("insert into sync_meta(k,v) values (?,?)",("scan_file_size",total_file_size))
        cx.commit()
        
    if dir_count: #存入文件夹列表到数据库
        c = cx.cursor()
        sql = """insert into src_tree(is_dir,file_path,virtual_path,last_mod_time,file_size,local_time) values (?,?,?,?,?,?)"""
        c.executemany(sql,dir_list)
        #更新扫描到的文件夹数
        c.execute("insert into sync_meta(k,v) values (?,?)",("scan_dir_count",dir_count))
        cx.commit()
        
   
def need_delete_list(cx):
    sql = """select p.is_dir,p.file_path,p.virtual_path,p.last_mod_time,p.file_size from snapshot p where p.virtual_path not in (select t.virtual_path from src_tree t )"""
    c = cx.cursor()
    c.execute(sql)
    need_delete_lst = []
    for line in c.fetchall():
        need_delete_lst.append((line[0],line[1],line[2],line[3],line[4]))
    #add by danqian begin    
    srcDirSize = getRelSize(src_dir)
    dstDirSize = getRelSize(dst_dir)
    if srcDirSize==0L and dstDirSize!=0L: #add by danqian end
        need_delete_lst = []
    return need_delete_lst
       
def need_sync_list(cx):    
    sync_list_new_add = []
    sync_list_updated = []
    sql_add = """
            select t.is_dir, t.file_path,t.virtual_path,t.last_mod_time,t.file_size from src_tree t where t.virtual_path not in (select p.virtual_path from snapshot p)
            """ #新增文件,文件夹 
    c = cx.cursor()
    c.execute(sql_add)
    for line in c.fetchall():
        sync_list_new_add.append((line[0],line[1],line[2],line[3],line[4]))

    sql_updated = """
            select t.is_dir, t.file_path,t.virtual_path,t.last_mod_time,t.file_size from src_tree t,snapshot p where t.virtual_path=p.virtual_path and t.last_mod_time > p.last_mod_time and t.is_dir='N'
            """#更改过的文件
    c.execute(sql_updated)
    for line in c.fetchall():
        sync_list_updated.append((line[0],line[1],line[2],line[3],line[4]))        
    return sync_list_new_add,sync_list_updated

def get_filesize_from_snapshot(cx,virtual_path):
    file_size = 0
    c = cx.cursor()
    sql = "select file_size from snapshot where virtual_path=?"
    c.execute(sql,(virtual_path,))
    line = c.fetchone()
    file_size = line[0]
    
    

def update_snapshot(cx,is_dir,file_path,virtual_path,last_mod_time,file_size,time_now,action_flag):
    c = cx.cursor()
    sql_log = """insert into sync_log(is_dir,file_path,virtual_path,last_mod_time,file_size,task_type,local_time) values (?,?,?,?,?,?,?)"""  
    if action_flag == "new_add":
        sql = """insert into snapshot(is_dir,file_path,virtual_path,last_mod_time,file_size,local_time) values (?,?,?,?,?,?)"""
        param = (is_dir,file_path,virtual_path,last_mod_time,file_size,time_now)
        action_name = "AddFile"
        if is_dir == "Y":
            action_name = "AddDir"
            c.execute("update sync_meta set v = v+1 where k = 'sync_success_dir_count'")
        else:
            c.execute("update sync_meta set v = v+1 where k = 'sync_success_file_count'")
            c.execute("update sync_meta set v = v+? where k = ? ",(file_size,"sync_success_file_size"))
            
        param_log = (is_dir,file_path,virtual_path,last_mod_time,file_size,action_name,time_now)
    else:
        sql = """update snapshot set last_mod_time = ? , file_size = ?  where virtual_path = ?"""
        param = (last_mod_time,file_size,virtual_path)    
        param_log = (is_dir,file_path,virtual_path,last_mod_time,file_size,"UpdatedFile",time_now)
        #更新文件时，总量加上增量
        old_file_size = get_filesize_from_snapshot(cx,virtual_path)
        increment_file_size = file_size - old_file_size        
        c.execute("update sync_meta set v = v+? where k = ? ",(increment_file_size,"sync_success_file_size"))
        
    c.execute(sql,param)
    c.execute(sql_log,param_log)    
    c.execute("update sync_meta set v = ? where k = 'last_sync_time' ",(time_now,)) #更新最后一次同步时间
    cx.commit()
    
    
def update_remote_sync_meta(conn,cx):       
    try:
        c = cx.cursor()
        c.execute("select k,v from sync_meta") 
        meta_dict = {}
        for line in c.fetchall():
            meta_dict[line[0]] = line[1]            
        
        sql = "update sync_meta set sync_file_size=%s,sync_file_count=%s,sync_dir_count=%s,last_sync_time=%s where username=%s "
        
        c_r = conn.cursor()
        sync_file_size = float(meta_dict.get("sync_success_file_size",0))
        sync_file_count = int(meta_dict.get("sync_success_file_count",0))
        sync_dir_count = int(meta_dict.get("sync_success_dir_count",0))
        last_sync_time = meta_dict.get("last_sync_time","")
        c_r.execute(sql,(sync_file_size,sync_file_count,sync_dir_count,last_sync_time,username))
        conn.commit()        
    except:
        pass
    
def sync_delete_dst(conn,cx,need_delete_lst):
    action_name="DeleteFile"
    c = cx.cursor()
    sql_log = """insert into sync_log(is_dir,file_path,virtual_path,last_mod_time,file_size,task_type,local_time) values (?,?,?,?,?,?,?)"""
    time_now = datetime.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M:%S")    
    if len(need_delete_lst):
        for is_dir,file_path,virtual_path,last_mod_time,file_size in need_delete_lst:
            dst_path = dst_dir+os.sep+virtual_path
            if is_dir == "Y":
                shutil.rmtree(dst_path,True)# 删除远端磁盘上的文件夹
                action_name = "DeleteDir"
                c.execute("update sync_meta set v = v-1 where k = 'sync_success_dir_count'")
            else:
                os.remove(dst_path)# 删除远端磁盘上的文件
                c.execute("update sync_meta set v = v-1 where k = 'sync_success_file_count'")
                c.execute("update sync_meta set v = v-? where k = ? ",(file_size,"sync_success_file_size"))
                
               
            c.execute("""delete from snapshot where virtual_path=?""",(virtual_path,) )
            c.execute(sql_log,(is_dir,file_path,virtual_path,last_mod_time,file_size,action_name,time_now)) #从snapshot表删除            
            c.execute("update sync_meta set v = ? where k = 'last_sync_time' ",(time_now,)) #更新最后一次同步时间          
            cx.commit()
        update_remote_sync_meta(conn,cx) #更新远程DB数据

def robosync(conn,cx,sync_list,action_flag):
    time_now = datetime.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M:%S")
    for is_dir,from_file,virtual_path,last_mod_time,file_size in sync_list:
        temp_from_file = from_file.replace(src_dir,dst_dir,1)
        if is_dir == "Y":
            if not os.path.exists(temp_from_file):
                os.makedirs(temp_from_file)
        else:
            to_dir = os.path.dirname(temp_from_file)
            if not os.path.exists(to_dir):
                os.makedirs(to_dir)
            try:
                shutil.copy2(from_file, to_dir)
            except:
                pass
        #更新snapshot
        update_snapshot(cx,is_dir,from_file,virtual_path,last_mod_time,file_size,time_now,action_flag)
        update_remote_sync_meta(conn,cx) #更新远程DB数据
        
        
def getRelSize(destDir):   
    total_file_size = 0
    for root,dirs,files in os.walk(destDir):        
        for xfile in files: #获得所有文件
            file_path = os.path.join(root,xfile)
            if os.path.dirname(file_path) not in DIR_EXCLUDES:
                file_attr = os.stat(file_path)                               
                file_size = round(file_attr.st_size,1) #精确到0.1KB            
                total_file_size = total_file_size + file_size    
    return total_file_size

    
def run(conn,cx):
    cleart_db(cx)
    scan_src_tree(cx)
    sync_list_new_add,sync_list_updated = need_sync_list(cx) 
    sync_delete_list = need_delete_list(cx)
    if len(sync_list_new_add):
        robosync(conn,cx,sync_list_new_add,"new_add")
    if len(sync_list_updated):
        robosync(conn,cx,sync_list_updated,"new_update")
    if len(sync_delete_list):
        sync_delete_dst(conn,cx,sync_delete_list)
        
    
        
    


def main():
    try:       
        conn = get_conn()
        remote_server = get_remote_server(conn)
        global dst_dir #此处使用了全局变量，特别注意
        dst_dir = dst_dir.replace("sync_server",remote_server,1)
        flag = init_check()#check 是否有源和目标文件夹
        if flag:
            init_config()
            cx = get_cx()
            run(conn,cx)
            cx.close()
        conn.close()
    except:
        pass
    
def mainx():
    conn = get_conn()
    remote_server = get_remote_server(conn)
    global dst_dir #此处使用了全局变量，特别注意
    dst_dir = dst_dir.replace("sync_server",remote_server,1)
    flag = init_check()#check 是否有源和目标文件夹
    if flag:
        init_config()
        cx = get_cx()
        run(conn,cx)
        cx.close()
    conn.close()

    


        
   
if __name__ == '__main__':
    mainx()
