import numpy as np
import sqlalchemy as sq
import bs4 
import requests as rq
import pandas as pd
import time as t
import datetime 
import random
print('import successful')
nom=900291
while nom<999999:
    r=random.randrange(1,2)
    res=rq.get('https://bina.az/items/'+str(nom))
    if str(res)!='<Response [200]>':
        print('request error',str(res))
        pass
    else:
        res=rq.get('https://bina.az/items/' +str(nom))
        soup=bs4.BeautifulSoup(res.text,'lxml')
        alert=soup.find_all('p',{'class':"flash"}) 
        print(datetime.datetime.now(),nom)
        try:
            alert[0].text
        except Exception as d:
            error=str(d)
        print(error)
        price_full=""
        id=0
        while error.find("index out of range")!=-1 and id<1:
            ad,phone,lables=[],[],[]
            print("while isleyir")
            name=soup.find('li',{'class':"name"})  
            tel=soup.find_all('a',{'class':"phone"}) 
            price=soup.find_all('span',{'class':"price-val"})
            price_cur=soup.find_all('span',{'class':"price-cur"})
            error=""
            try:
                price_full= price[0].text+" "+price_cur[0].text
            except Exception as l:
                error=str(l)
            if error.find("index out of range")!=-1:
                pass
            else:
                pass
            error=""
            try:
                phone.append(tel[0].text)
            except Exception as s:
                error=str(s)
            if error.find("index out of range")!=-1:
                phone.append(None)
                pass
            else:
                #phone.append(tel[0].text)
                pass
            print(price_full,phone,nom)
            table_df = pd.DataFrame({'Load_date': datetime.datetime.now(),'Amount':price_full, 
                                     'Phone':phone,'Elan_nom':nom})
            engine=sq.create_engine('sqlite:///direction') #burada oz database inizin yerini göstərin
            table_df.to_sql('table_data',con=engine,if_exists='append') 
            id=id+1
    nom=nom+1
    t.sleep(r)
    from gc import  collect
    collect()
