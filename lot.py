# -*- coding: utf-8 -*-

from pyquery import PyQuery as pq
import datetime as dt
import time
import requests
import pandas as pd
import pymysql
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine

LOT_URL = "http://83111.com/index.php?c=content&a=list&catid={0}&day={1}"
LOT_TYPE = {'index':['204','9','26'],'attr':[ ['幸运飞艇',10,'.ball_pks_'],['北京pk10',10,'.ball_pks_'],['重庆时时彩',5,'.ball_s_'] ] }  #id,彩种名称,位数

HEADERS = {'ua': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.125 Safari/537.36'}
showSql = False
cfg = 'mysql://root:123456@localhost/quant?charset=utf8'

def getEngin(config=None):
    return create_engine(cfg if config==None else config)

def query(sql):
    if showSql:
        print(sql)
    conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='123456', db='quant',charset='utf8')
    cursor = conn.cursor()
    cursor.execute(sql)
 
    d = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close()
    return d
	
def exe(sql):
    if showSql:
        print(sql)
    conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='123456', db='quant',charset='utf8')
    cursor = conn.cursor()
    r = cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()	
    
    return r
    
def don():
    return

def getAttrByCid(cid='204'):
    lottype_idx = LOT_TYPE['index'].index(cid)
    name = LOT_TYPE['attr'][lottype_idx][0] #彩种名称
    css_class = LOT_TYPE['attr'][lottype_idx][2]  #样式名
    bit = LOT_TYPE['attr'][lottype_idx][1]  #位数

    return [name,bit,css_class]

#获取历史数据（默认当日）
def getlot(day=dt.datetime.now().strftime('%Y-%m-%d'),cid='204'):
    url = LOT_URL.format(cid,day)
    resp = requests.get(url,headers=HEADERS,timeout=(3,60))
    #print(url)
    attrs = getAttrByCid(cid)
    name = attrs[0] #彩种名称
    css_class = attrs[2]  #样式名
    bit = attrs[1]  #位数
    doc = pq(resp.text) #pq(url=url) 
    balls = doc(css_class)
    #print(type(balls),len(balls))
    #print(resp.text)
    arr = []
    arrDX = []
    arrXD = []
    periods = int(len(balls)/bit)  #周期数量
    print(name,url)
    for i in range(periods):
        a = ['qs',cid,day,periods-i]  #期数/cid/开奖时间／场次／
        d = []
        x = []
        for j in range(bit):
            s1 = pq(balls[i*bit+j]).attr('title')
            s2 = str(int(s1)%2)
            s3 = '0' if int(s1)<6 else '1'
            #print(s1,s2,s3)
            a.append(s1)
            d.append(s2)
            x.append(s3)
        arr.append(a)
        arrDX.append(d)
        arrXD.append(x)
        
    return {'arr':arr,'arrDX':arrDX,'arrXD':arrXD}


#写入历史数据
def putlot(start=None,offset=360,cid='26',flag='append'):
    s = dt.datetime.strptime(start,("%Y-%m-%d")) if not start==None else dt.datetime.now()
    dd = []

    attrs = getAttrByCid(cid)
    name = attrs[0] #彩种名称
    css_class = attrs[2]  #样式名
    bit = attrs[1]  #位数

    columns = ['qs','cid','date','idx']

    for n in range(bit):
        columns.append('no'+str(n+1))

    print(columns)

    for i in range(offset):
        ss = s - dt.timedelta(days=i)
        print(ss)
        d = getlot(ss.strftime('%Y-%m-%d'),cid)
        df = pd.DataFrame(list(d['arr']),columns=columns)
        df.to_sql('lot',getEngin(),if_exists=flag)

        dx1  =  ''.join([x[0] for x in d['arrDX'][::-1]]) if bit > 0 else ''
        dx2  =  ''.join([x[1] for x in d['arrDX'][::-1]]) if bit > 1 else ''
        dx3  =  ''.join([x[2] for x in d['arrDX'][::-1]]) if bit > 2 else ''
        dx4  =  ''.join([x[3] for x in d['arrDX'][::-1]]) if bit > 3 else ''
        dx5  =  ''.join([x[4] for x in d['arrDX'][::-1]]) if bit > 4 else ''
        dx6  =  ''.join([x[5] for x in d['arrDX'][::-1]]) if bit > 5 else ''
        dx7  =  ''.join([x[6] for x in d['arrDX'][::-1]]) if bit > 6 else ''
        dx8  =  ''.join([x[7] for x in d['arrDX'][::-1]]) if bit > 7 else ''
        dx9  =  ''.join([x[8] for x in d['arrDX'][::-1]]) if bit > 8 else ''
        dx10 =  ''.join([x[9] for x in d['arrDX'][::-1]]) if bit > 9 else ''

        xd1  =  ''.join([x[0] for x in d['arrXD'][::-1]]) if bit > 0 else ''
        xd2  =  ''.join([x[1] for x in d['arrXD'][::-1]]) if bit > 1 else ''
        xd3  =  ''.join([x[2] for x in d['arrXD'][::-1]]) if bit > 2 else ''
        xd4  =  ''.join([x[3] for x in d['arrXD'][::-1]]) if bit > 3 else ''
        xd5  =  ''.join([x[4] for x in d['arrXD'][::-1]]) if bit > 4 else ''
        xd6  =  ''.join([x[5] for x in d['arrXD'][::-1]]) if bit > 5 else ''
        xd7  =  ''.join([x[6] for x in d['arrXD'][::-1]]) if bit > 6 else ''
        xd8  =  ''.join([x[7] for x in d['arrXD'][::-1]]) if bit > 7 else ''
        xd9  =  ''.join([x[8] for x in d['arrXD'][::-1]]) if bit > 8 else ''
        xd10 =  ''.join([x[9] for x in d['arrXD'][::-1]]) if bit > 9 else ''

        dd = [[ss.strftime('%Y-%m-%d'),cid,dx1,dx2,dx3,dx4,dx5,dx6,dx7,dx8,dx9,dx10,xd1,xd2,xd3,xd4,xd5,xd6,xd7,xd8,xd9,xd10]]
        df2 = pd.DataFrame(list(dd),columns=['date','cid','dx1', 'dx2','dx3','dx4','dx5','dx6','dx7','dx8','dx9','dx10','xd1', 'xd2','xd3','xd4','xd5','xd6','xd7','xd8','xd9','xd10']) 
        df2.to_sql('lot2',getEngin(),if_exists=flag)

        percent = 1.0 * i / offset * 100
        print('complete percent:%10.8s%s'%(str(round(percent,2)),'%'),end='\r') 
        time.sleep(0.1) 

def countSeq(TF):
    result = []
    if TF is None or len(TF) == 0:
        return result
    pattern = TF[0]
    count = 1
    for s in TF[1:]:
        if s == pattern:
            count += 1
        else:
            result.append(pattern + ':' + str(count))
            pattern = s
            count = 1
    result.append(pattern + ':' + str(count))
    return result

def printSeq(TF):
    result = []
    if TF is None or len(TF) == 0:
        return result
    pattern = TF[0]
    result.append(pattern)
    for s in TF[1:]:
        if s == pattern:
            result[-1] += s
        else:
            pattern = s
            result.append(pattern)
    return result    

#输出分析数据
def analy(a=0):
    s = 'select * from lot2'
    d = query(s)
    df = pd.DataFrame(list(d),columns=['index','cid','date','dx1', 'dx2','dx3','dx4','dx5','dx6','dx7','dx8','dx9','dx10','xd1', 'xd2','xd3','xd4','xd5','xd6','xd7','xd8','xd9','xd10'])
    
    arr = []
    arr2 = []

    for i,o in df.iterrows():
        arr.append([o[2].strftime('%Y-%m-%d'),i+1,countSeq(o[1+2])])
        arr.append([o[2].strftime('%Y-%m-%d'),i+1,countSeq(o[1+3])])
        arr.append([o[2].strftime('%Y-%m-%d'),i+1,countSeq(o[1+4])])
        arr.append([o[2].strftime('%Y-%m-%d'),i+1,countSeq(o[1+5])])
        arr.append([o[2].strftime('%Y-%m-%d'),i+1,countSeq(o[1+6])])
        arr.append([o[2].strftime('%Y-%m-%d'),i+1,countSeq(o[1+7])])
        arr.append([o[2].strftime('%Y-%m-%d'),i+1,countSeq(o[1+8])])
        arr.append([o[2].strftime('%Y-%m-%d'),i+1,countSeq(o[1+9])])
        arr.append([o[2].strftime('%Y-%m-%d'),i+1,countSeq(o[1+10])])
        arr.append([o[2].strftime('%Y-%m-%d'),i+1,countSeq(o[1+11])])

        arr2.append([o[2].strftime('%Y-%m-%d'),i+1,countSeq(o[1+12])])
        arr2.append([o[2].strftime('%Y-%m-%d'),i+1,countSeq(o[1+13])])
        arr2.append([o[2].strftime('%Y-%m-%d'),i+1,countSeq(o[1+14])])
        arr2.append([o[2].strftime('%Y-%m-%d'),i+1,countSeq(o[1+15])])
        arr2.append([o[2].strftime('%Y-%m-%d'),i+1,countSeq(o[1+16])])
        arr2.append([o[2].strftime('%Y-%m-%d'),i+1,countSeq(o[1+17])])
        arr2.append([o[2].strftime('%Y-%m-%d'),i+1,countSeq(o[1+18])])
        arr2.append([o[2].strftime('%Y-%m-%d'),i+1,countSeq(o[1+19])])
        arr2.append([o[2].strftime('%Y-%m-%d'),i+1,countSeq(o[1+20])])
        arr2.append([o[2].strftime('%Y-%m-%d'),i+1,countSeq(o[1+21])])

    return {'jiou':arr} if a==0 else {'daxiao':arr2}

def buyFlag(strategy):
    return strategy

def bingo(strategy):
    return strategy


#策略
st = {'trade':['0000001:1','1111110:0'],'balance':15000,'bet':10,'case':'00000000111111100000000111111101110101','log':[]}

#模拟操作
def mony(strategy=None,flag='双单'):
    strategy = st if strategy==None else strategy
    balance = strategy['balance']
    bet = strategy['bet']
    case = strategy['case']

    for x in st['trade']:
        when = x.split(':')[0]
        buyFlag = x.split(':')[1]
        if when in case:
            tail = case[case.index(when)+len(when):]
            print(' 当 : ',when,',你买¥',bet,'的',flag[int(buyFlag)],',接下来开盘：',tail)
            b = balance
            for i in range(len(tail)):
                if tail[i] != buyFlag :
                    b -= bet
                    kui = (balance-b)
                    bet = kui*2
                    s = '{4}:  {0}-{1}={2} next:{3}'.format(balance,kui,b,bet,i+1)
                    print(s)
                    if bet>b:
                        strategy['balance'] = bet-b*1.99
                        print('Boom,爆仓了!!!你的余额仅有 :',strategy['balance'],',而你最好要有 :' ,kui+bet )
                        bet = strategy['bet']
                        
                        break
                else :
                    b += bet*1.99
                    z = b-balance
                    print(i+1,'bingo','本期赚了: ', z, ' ,帐户余额: ', b,',本次开销：',bet*2)
                    strategy['case'] = tail
                    bet = strategy['bet']
                    strategy['balance'] = b
                    if when in tail :
                        mony(strategy)
                    break
        else:
            break


def globalmony(cid=None):
    sql = """
        select cid,date,dx1,dx2,dx3,dx4,dx5,dx6,dx7,dx8,dx9,dx10, xd1,xd2,xd3,xd4,xd5,xd6,xd7,xd8,xd9,xd10 
        from lot2 
        {1} where cid='{0}' 
        order by cid,date
    """
    sql = sql.format(cid,'#' if cid==None else '')
    #print(sql)
    d = query(sql)
    print(st)
    for i in range(len(d)):
        print('彩种:',d[i][0],'日期:',d[i][1])
        for j in range(2,len(d[i])):
            flag = '小大' if j>11 else '双单'
            st['case'] = d[i][j]
            mony(st,flag=flag)
            print('\r\n')

DINGDING_URL = 'https://oapi.dingtalk.com/robot/send?access_token=7f36bb869233f0cf4e5f8c86857b0001b7ec401c8582f8d5bbe17e5d126b9e31'
def sendMsg(msg,title='这是一个来自钉钉的消息'):
    data={"msgtype":"text","text":{"content":msg,"title":title}}
    try:
        resp = requests.post(DINGDING_URL,headers=HEADERS,json=data,timeout=(3,60))
    except:
        print ("Send DingDing Message failed!");

#关于定时器设置可参考http://jinbitou.net/2016/12/19/2263.html
from apscheduler.schedulers.blocking import BlockingScheduler

sched = BlockingScheduler()

#每n秒执行
@sched.scheduled_job('interval', max_instances=1,id='listen_job', seconds=120) 
def listen():
    try:
        d = getlot()

        dx1  =  ''.join([x[0] for x in d['arrDX'][::-1]])
        dx2  =  ''.join([x[1] for x in d['arrDX'][::-1]])
        dx3  =  ''.join([x[2] for x in d['arrDX'][::-1]])
        dx4  =  ''.join([x[3] for x in d['arrDX'][::-1]])
        dx5  =  ''.join([x[4] for x in d['arrDX'][::-1]])
        dx6  =  ''.join([x[5] for x in d['arrDX'][::-1]])
        dx7  =  ''.join([x[6] for x in d['arrDX'][::-1]])
        dx8  =  ''.join([x[7] for x in d['arrDX'][::-1]])
        dx9  =  ''.join([x[8] for x in d['arrDX'][::-1]])
        dx10 =  ''.join([x[9] for x in d['arrDX'][::-1]])

        xd1  =  ''.join([x[0] for x in d['arrXD'][::-1]])
        xd2  =  ''.join([x[1] for x in d['arrXD'][::-1]])
        xd3  =  ''.join([x[2] for x in d['arrXD'][::-1]])
        xd4  =  ''.join([x[3] for x in d['arrXD'][::-1]])
        xd5  =  ''.join([x[4] for x in d['arrXD'][::-1]])
        xd6  =  ''.join([x[5] for x in d['arrXD'][::-1]])
        xd7  =  ''.join([x[6] for x in d['arrXD'][::-1]])
        xd8  =  ''.join([x[7] for x in d['arrXD'][::-1]])
        xd9  =  ''.join([x[8] for x in d['arrXD'][::-1]])
        xd10 =  ''.join([x[9] for x in d['arrXD'][::-1]])

        for w in st['when']:
            l = len(w)
            sendMsg(dx1 ,'奇偶:'+w) if w==dx1  [-l:] else don()
            sendMsg(dx2 ,'奇偶:'+w) if w==dx2  [-l:] else don()
            sendMsg(dx3 ,'奇偶:'+w) if w==dx3  [-l:] else don()
            sendMsg(dx4 ,'奇偶:'+w) if w==dx4  [-l:] else don()
            sendMsg(dx5 ,'奇偶:'+w) if w==dx5  [-l:] else don()
            sendMsg(dx6 ,'奇偶:'+w) if w==dx6  [-l:] else don()
            sendMsg(dx7 ,'奇偶:'+w) if w==dx7  [-l:] else don()
            sendMsg(dx8 ,'奇偶:'+w) if w==dx8  [-l:] else don()
            sendMsg(dx9 ,'奇偶:'+w) if w==dx9  [-l:] else don()
            sendMsg(dx10,'奇偶:'+w) if w==dx10 [-l:] else don()
                
            sendMsg(xd1 ,'大小:'+w ) if w==xd1 [-l:] else don()
            sendMsg(xd2 ,'大小:'+w ) if w==xd2 [-l:] else don()
            sendMsg(xd3 ,'大小:'+w ) if w==xd3 [-l:] else don()
            sendMsg(xd4 ,'大小:'+w ) if w==xd4 [-l:] else don()
            sendMsg(xd5 ,'大小:'+w ) if w==xd5 [-l:] else don()
            sendMsg(xd6 ,'大小:'+w ) if w==xd6 [-l:] else don()
            sendMsg(xd7 ,'大小:'+w ) if w==xd7 [-l:] else don()
            sendMsg(xd8 ,'大小:'+w ) if w==xd8 [-l:] else don()
            sendMsg(xd9 ,'大小:'+w ) if w==xd9 [-l:] else don()
            sendMsg(xd10,'大小:'+w ) if w==xd10[-l:] else don()

    except Exception as e:
        sched.shutdown()

def days_diff(str1,str2):
    date1=datetime.datetime.strptime(str1[0:10],"%Y-%m-%d")
    date2=datetime.datetime.strptime(str2[0:10],"%Y-%m-%d")
    num=(date1-date2).days
    return num

#补全数据
@sched.scheduled_job('cron', id='job_completData', hour='12')
def completData():
    now = dt.datetime.now()
    sql = 'select date from lot2 order by date desc limit 1'
    newest = query(sql)[0][0].strftime('%Y-%m-%d')
    diff = (now - dt.datetime.strptime(newest,("%Y-%m-%d"))).days - 1
    yesterday = (now - dt.timedelta(days=1)).strftime('%Y-%m-%d')
    print(newest,yesterday,diff)

    putlot(yesterday,diff)

import sys  
if __name__ == '__main__':
    if len(sys.argv) > 1 :
        if sys.argv[1] == "day":
            print(getlot() if len(sys.argv)<3  else getlot(sys.argv[2]))
        if sys.argv[1] == "all":
            print(analy(),analy(1))
        if sys.argv[1] == 'mony':
            mony()
        if sys.argv[1] == 'gm':
            globalmony()
        if sys.argv[1] == 'cp':
            completData()
        if sys.argv[1] == 'put':
            putlot()
    else:
        print('启动定时任务！')
        sched.start() 

