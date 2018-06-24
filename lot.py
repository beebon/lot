# -*- coding: utf-8 -*-

from pyquery import PyQuery as pq
import datetime as dt
import time
import requests
import pandas as pd
import pymysql
pymysql.install_as_MySQLdb()
import re
from sqlalchemy import create_engine

LOT_URL = "http://83111.com/index.php?c=content&a=list&catid={0}&day={1}"

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

# htm = """
#         <td>2018-06-23 00:10:54</td>&#13;
#         <td> 20180623002 </td>&#13;
#         <td>&#13;
#         <span title="1" class="  ball_s_ ball_s_blue ball_lenght5  ">1</span>&#13;
#         <span title="1" class="  ball_s_ ball_s_blue ball_lenght5  ">1</span>&#13;
#         <span title="5" class="  ball_s_ ball_s_blue ball_lenght5  ">5</span>&#13;
#         <span title="1" class="  ball_s_ ball_s_blue ball_lenght5  ">1</span>&#13;
#         <span title="7" class="  ball_s_ ball_s_blue ball_lenght5  ">7</span>&#13;
#         </td>&#13;
#         <td class="count">15</td>&#13;
#         <td class="gray">单</td>&#13;
#         <td class="gray">小</td>&#13;
#         <td class="gray">虎</td>&#13;
# """

# def testHtml():
#     doc = pq(htm) 
#     balls = doc('.ball_s_')
#     print(pq(balls[0]).parent().prev().text())

def getAttrByCid(cid='204'):
    lottype_idx = LOT_TYPE['index'].index(cid)
    name = LOT_TYPE['attr'][lottype_idx][0] #彩种名称
    css_class = LOT_TYPE['attr'][lottype_idx][2]  #样式名
    bit = LOT_TYPE['attr'][lottype_idx][1]  #位数

    return [name,bit,css_class]

LOT_TYPE = {'index':['204','9','26','34','44'],'attr':[ ['幸运飞艇',10,'.ball_pks_'],['北京pk10',10,'.ball_pks_'],['重庆时时彩',5,'.ball_s_'],['广东快乐十分',8,'.ball_s_'],['重庆幸运农场',8,'.ball_nc_'] ] }  #id,彩种名称,位数

#获取历史数据（默认当日）
def getlot(day=dt.datetime.now().strftime('%Y-%m-%d'),cid='204'):
    url = LOT_URL.format(cid,day)
    resp = requests.get(url,headers=HEADERS,timeout=(10,60))  #10次，每次60秒
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
    #print(name,url)
    for i in range(periods):
        qs = pq(balls[i*bit]).parent().parent().prev().text() if bit==10 else pq(balls[i*bit]).parent().prev().text()
        dt = pq(balls[i*bit]).parent().parent().prev().prev().text() if bit==10 else pq(balls[i*bit]).parent().prev().prev().text()
        #print('qs:',qs,'dt',dt,'end')
        a = [qs,cid,dt,periods-i]  #期数/cid/开奖时间／场次／
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
def putlot(start=None,offset=360,cid='204',flag='append'):
    try:
        print(cid,start)
        s = dt.datetime.strptime(start,("%Y-%m-%d")) if not start==None else dt.datetime.now()
        dd = []

        attrs = getAttrByCid(cid)
        name = attrs[0] #彩种名称
        css_class = attrs[2]  #样式名
        bit = attrs[1]  #位数

        columns = ['qs','cid','date','idx']

        for n in range(bit):
            columns.append('no'+str(n+1))

        #print(columns)

        for i in range(offset):
            ss = s - dt.timedelta(days=i)
            #print(ss)
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

            if dx1!='':
                dd = [[ss.strftime('%Y-%m-%d'),cid,dx1,dx2,dx3,dx4,dx5,dx6,dx7,dx8,dx9,dx10,xd1,xd2,xd3,xd4,xd5,xd6,xd7,xd8,xd9,xd10]]
                df2 = pd.DataFrame(list(dd),columns=['date','cid','dx1', 'dx2','dx3','dx4','dx5','dx6','dx7','dx8','dx9','dx10','xd1', 'xd2','xd3','xd4','xd5','xd6','xd7','xd8','xd9','xd10']) 
                df2.to_sql('lot2',getEngin(),if_exists=flag)

            percent = 1.0 * i / offset * 100
            print('complete percent:%10.8s%s'%(str(round(percent,2)),'%'),end='\r') 
            time.sleep(0.1) 
    except Exception as e:
        print(start,cid,error)

def putAll(start=None,offset=100):
    for cid in LOT_TYPE['index']:
        putlot(start,offset,cid=cid)

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
def analy(a=0,cid=26):
    s = 'select * from lot2' if cid==None else "select * from lot2 where cid='{0}'".format(cid)
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

    return {'单双':arr} if a==0 else {'大小':arr2}

#获取期数信息
def getQsInfo(cid='26',idx=0,date=dt.datetime.now().strftime('%Y-%m-%d')):
    sql = """
        select qs,DATE_FORMAT(date,"%Y-%m-%d %T") from lot
        where cid='{0}' and idx={1} AND DATE_FORMAT(date,'%Y-%m-%d')='{2}'
    """
    sql = sql.format(cid,idx,date)
    #print(sql)
    d = query(sql)
    return(d)

#策略
st = {'trade':['0000001:0','1111110:1'],'balance':400000,'bet':2,'case':'00000000111111100000000111111101110101','rate':2,'notEnough':'stop'}

#模拟操作
def mony(strategy=None,flag='双单',flagNo=None):
    strategy = st if strategy==None else strategy
    case = strategy['case']        #开盘情况
    rate = strategy['rate']        #下注额        
    balance = strategy['balance']  #余额
    bet = strategy['bet']          #下注额

    for x in st['trade']:  #循环策略
        when = x.split(':')[0]       #命中标记
        lst = [i.start() for i in re.finditer(when, case)] #命中索引列表
        buyFlag = x.split(':')[1]    #中奖标记
        bets = [bet]

        for idx in lst:
            if when in case:        #当命中时
                tail = case[idx+len(when):]  #开盘
                bets = [strategy['bet']]
                cost = strategy['bet']
                qs = getQsInfo(strategy['cid'],idx+1,strategy['date'])
                print('☆☆☆☆☆ 期数:',idx+1,qs,'当前余额：',strategy['balance'],',当:',when.replace('0',flag[0]).replace('1',flag[1]),'时下注',flag[int(buyFlag)],flagNo,
                      ';开盘情况：',tail[0:12].replace('0',flag[0]).replace('1',flag[1]),'☆☆☆☆☆')
                
                for i in range(len(tail)):
                    bet = bets[i]

                    if tail[i] != buyFlag :   #当不符合中奖点时
                        print('✕第{0}轮下注{1}元;余额={2}-{1}={3}'.format(i+1,bet,strategy['balance'],strategy['balance']-bet))#b,kui,b-kui,bet,
                        bets.append(sum(bets)*rate)
                        strategy['balance'] -= bet

                        if cost > strategy['balance'] : #当成本超出策略额度时
                            print('  ╳第{0}轮Boom,你需要至少{1},当前额度只有{2}'.format(i+1,cost,strategy['balance']))
                        cost = sum(bets)
                        #print(cost)
                        #print('下一注：',bet,'成本：',cost)
                        # b = balance - bet
                        # strategy['balance'] = b
                        # #print(s,'kui',kui,'=',balance,'-',b)
                        # if bet>b:  #当余额不足以支付下一注金额时
                        #     strategy['balance'] = 0
                        #     print('✕第{0}轮下注：'.format(i+2),b,'，Boom,爆仓了!!!你的余额仅有 :',0,',而你最好要有 :' ,bet*2,'\r\n' )
                        #     bet = strategy['bet']
                        #     break
                      
                    else :                  #当符合中奖点时
                        # bet += int(bet*rate)
                        # bingo = int(balance-bet)
                        # print('√第{0}轮下注'.format(i+1),bet,'Bingo！！！本期赚了: {1},余额为{0}'.format(balance,bingo) , ',本次开销：',bet*2,'\r\n')
                        # bet = strategy['bet']
                        # strategy['balance'] = balance
                        #判断余额是否够下一注
                        bet = bet if strategy['balance'] > bet else strategy['balance']
                        strategy['balance'] = (strategy['balance'] if strategy['balance'] > bet else 0) - bet + bet*rate
                        # if strategy['balance'] > bet:
                        #     cost 
                        print('√第{0}轮下注{1}元;余额:{2}'.format(i+1,bet,strategy['balance']),'成本：',cost)#b,kui,b-kui,bet,
                        bets.append(sum(bets)*rate)
                        break


def globalmony(cid='26'):
    attr = getAttrByCid(cid)
    sql = """
        select cid,date,dx1,dx2,dx3,dx4,dx5,dx6,dx7,dx8,dx9,dx10, xd1,xd2,xd3,xd4,xd5,xd6,xd7,xd8,xd9,xd10 
        from lot2 
        {1} where cid='{0}' 
        order by cid,date
    """
    sql = sql.format(cid,'#' if cid==None else '')
    #print(sql)
    d = query(sql)
    print('你的策略：',st,'\r\n')
    for i in range(len(d)):
        b = st['balance']
        print('---------彩种:',attr[0],'日期:',d[i][1],'余额：',b,'-------')
        for j in range(2,len(d[i])):
            flag = '小大' if j>11 else '双单' #'小大{0}'.format(j-2) if j>11 else '双单{0}'.format(j-12)
            st['case'] = d[i][j]
            st['cid'] = d[i][0]
            st['date'] = d[i][1]
            mony(st,flag,(j-1)%10)

        print('{0}该日约可赚：'.format(d[i][1]),st['balance'] - b,'\r\n')

def monyAll():
    for cid in LOT_TYPE['index']:
        globalmony(cid)

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
def listen(cid='26'):
    # try:
        d = getlot() if cid==None else getlot(cid)

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

        for w in st['trade']:
            f = w.split(':')[0]
            l = len(f)
            sendMsg(cid + '>奇偶1 :' + f) if f==dx1  [-l:] else don()
            sendMsg(cid + '>奇偶2 :' + f) if f==dx2  [-l:] else don()
            sendMsg(cid + '>奇偶3 :' + f) if f==dx3  [-l:] else don()
            sendMsg(cid + '>奇偶4 :' + f) if f==dx4  [-l:] else don()
            sendMsg(cid + '>奇偶5 :' + f) if f==dx5  [-l:] else don()
            sendMsg(cid + '>奇偶6 :' + f) if f==dx6  [-l:] else don()
            sendMsg(cid + '>奇偶7 :' + f) if f==dx7  [-l:] else don()
            sendMsg(cid + '>奇偶8 :' + f) if f==dx8  [-l:] else don()
            sendMsg(cid + '>奇偶9 :' + f) if f==dx9  [-l:] else don()
            sendMsg(cid + '>奇偶10:' + f) if f==dx10 [-l:] else don()

            sendMsg(cid + '>大小1 :' + f ) if f==xd1 [-l:] else don()
            sendMsg(cid + '>大小2 :' + f ) if f==xd2 [-l:] else don()
            sendMsg(cid + '>大小3 :' + f ) if f==xd3 [-l:] else don()
            sendMsg(cid + '>大小4 :' + f ) if f==xd4 [-l:] else don()
            sendMsg(cid + '>大小5 :' + f ) if f==xd5 [-l:] else don()
            sendMsg(cid + '>大小6 :' + f ) if f==xd6 [-l:] else don()
            sendMsg(cid + '>大小7 :' + f ) if f==xd7 [-l:] else don()
            sendMsg(cid + '>大小8 :' + f ) if f==xd8 [-l:] else don()
            sendMsg(cid + '>大小9 :' + f ) if f==xd9 [-l:] else don()
            sendMsg(cid + '>大小10:' + f ) if f==xd10[-l:] else don()

    # except Exception as e:
    #     sched.shutdown()

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
        if sys.argv[1] == "get":
            print(getlot('2018-06-20') if len(sys.argv)<3  else getlot(sys.argv[2]))
        if sys.argv[1] == "analy":
            print(analy(),analy(1))
        if sys.argv[1] == 'mony':
            mony()
        if sys.argv[1] == 'gm':
            globalmony()
        if sys.argv[1] == 'ma':
            monyAll()
        if sys.argv[1] == 'cp':
            completData()
        if sys.argv[1] == 'put':
            putlot()
        if sys.argv[1] == 'putAll':
            putAll('2018-06-20')
        if sys.argv[1] == 'test':
            testHtml()
        if sys.argv[1] == 'ln':
            listen()
    else:
        print('启动定时任务！')
        sched.start() 

