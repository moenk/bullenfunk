#
# file: xetra.py
#
# coder: moenk
#
# required: selenium bindings and geckodriver for firefox
#

import sys
import time
import string
import locale
import psycopg2
import pyautogui
import pandas as pd
import numpy as np
from sklearn import linear_model
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from datetime import datetime, timedelta


# globale variable fuer die db verbindung
conn = None
def create_db():
    commands = (
        """ CREATE TABLE kurse (
            id SERIAL PRIMARY KEY,
            kurs_isin VARCHAR(25),
            kurs_date DATE,
            kurs_open FLOAT,
            kurs_close FLOAT,
            kurs_high FLOAT,
            kurs_low FLOAT,
            kurs_sales FLOAT,
            kurs_count FLOAT
        );
        """,
        """ CREATE TABLE aktien (
            id SERIAL PRIMARY KEY,
            aktien_isin VARCHAR(25),
            aktien_name VARCHAR(250),
            aktien_url VARCHAR(250),
            aktien_perf FLOAT,
            aktien_deter FLOAT,
            aktien_trend INTEGER,
            aktien_limit FLOAT,
            aktien_stop FLOAT
        );
        """,
        """ CREATE TABLE depot (
            id SERIAL PRIMARY KEY,
            depot_isin VARCHAR(25),
            depot_anzahl INTEGER,
            depot_stopkurs FLOAT,
            depot_stopdate DATE
        );
        """
        )
    global conn
    cur = conn.cursor()
    # create table one by one
    for command in commands:
        cur.execute(command)
    # close communication with the PostgreSQL database server
    cur.close()
    # commit the changes
    try:
        conn.commit()
    except:
        print (exception)
        sys.exit(1)


def open_db():
    global conn
    try:
        conn = psycopg2.connect("dbname='bullenfunk' user='postgres' host='localhost' password='pgadmin'")
    except:
        print ("I am unable to connect to the database")


def close_db():
    global conn
    if conn is not None:
        conn.close()



# holt nur die isin aller dax aktien
def hole_dax_aktien():
    cur = conn.cursor()
    driver = webdriver.Firefox()
    driver.get("http://www.xetra.com/xetra-de/instrumente/aktien/dax-aktien-auf-xetra")
    assert "DAX" in driver.title
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")
    # isin aus zweiten spalte der fehlerhaften tabelle
    for row in soup.find_all("tr")[1:]:
        cols = [td.get_text() for td in row.find_all("td")]
        aktien_name = cols[0]
        aktien_isin = cols[1]
        print (aktien_isin)
        # und rein damit
        try:
            if aktien_isin != "":
                command = "delete from aktien where aktien_isin = '" + aktien_isin + "';"
                cur.execute(command)
                command = "insert into aktien (aktien_isin,aktien_name) values ('" + aktien_isin + "','" + aktien_name + "');"
                print (command)
                cur.execute(command)
        except:
            print (exception)
            sys.exit(1)
    # und feierabend
    cur.close()
    conn.commit()
    driver.close()


# holt die isin aller xetra aktien
def hole_xetra_aktien(num_pages):
    # start robot instance
    driver = webdriver.Firefox()
    driver.get("http://www.xetra.com/xetra-de/instrumente/aktien/liste-der-handelbaren-aktien/xetra/")
    assert "Xetra" in driver.title
    # alle num_pages seiten durchgehen
    for i in range(1,num_pages):
        cur = conn.cursor()
        # naechste seite
        if i>1:
            time.sleep(5)
            elem = driver.find_element_by_class_name("icon-right-after")
            elem.click()
        # isin zu aktien holen
        time.sleep(5)
        elems = driver.find_elements_by_xpath("//p")
        # ISIN finden und speichern
        for elem in elems:
            aktien_isin=elem.text
            if "ISIN:" in aktien_isin:
                aktien_isin=aktien_isin[6:]
                try:
                    if aktien_isin != "":
                        command = "delete from aktien where aktien_isin = '" + aktien_isin + "';"
                        print (command)
                        cur.execute(command)
                        command = "insert into aktien (aktien_isin) values ('" + aktien_isin + "');"
                        print (command)
                        cur.execute(command)
                except:
                    print (exception)
                    sys.exit(1)
        cur.close()
        conn.commit()
    driver.close()


# holt die aktien url zur frankfurter boerse, aktualisiert titel
def hole_aktien_url(aktien_isin,aktien_url):
    global conn
    frankfurt_url = "http://www.boerse-frankfurt.de/"
    # muss die basis url erst noch geholt werden? oder ist sie nicht bekommen worden?
    if (aktien_url==None) or (aktien_url==frankfurt_url):
        # start robot instance
        driver = webdriver.Firefox()
        url = frankfurt_url
        driver.get(url)
        assert "Frankfurt" in driver.title
        # aktien isin holen
        elem = driver.find_element_by_name("_search")
        elem.clear()
        elem.send_keys(aktien_isin)
        time.sleep(2)
        elem.send_keys(Keys.RETURN);
        time.sleep(2)
        # url zur aktie holen und speichern
        aktien_url = driver.current_url
        try:
            aktien_name=driver.find_element_by_class_name("stock-headline").text
        except:
            aktien_name=""
            pass
        cur = conn.cursor()
        aktien_name=aktien_name.replace("'","")
        try:
            cur.execute("update aktien set (aktien_url,aktien_name) = (%s,%s) where aktien_isin=%s;",(aktien_url,aktien_name,aktien_isin))
            print (aktien_isin+": "+aktien_name)
        except:
            print (exception)
            sys.exit(1)
        cur.close()
        conn.commit()
        driver.close()
    else:
        # kein bedarf die aktien_url zu holen
        return False



# macht die bewertung der aktie und wird nach dem abruf historischer kurse aufgerufen
def aktien_rating(kurs_isin):
    # tage berechnen
    zeitrahmen = 233
    d2 = datetime.today() - timedelta(days=zeitrahmen)
    # alle kursdaten holen
    global conn
    command = "SELECT kurs_date, kurs_close from kurse where kurs_isin='" + kurs_isin + "' and kurs_date>'" + d2.strftime(
        "%d.%m.%Y") + "' order by kurs_date asc;"
    df = pd.read_sql(command, con=conn)

    # gleitender mittelwert der schlusskurse
    px = pd.DataFrame(df, columns=['kurs_date', 'kurs_close'])
    px['kurs_date'] = pd.to_datetime(px['kurs_date'])
    px.index = px['kurs_date']
    del px['kurs_date']
    px['ema1'] = px['kurs_close'].ewm(ignore_na=False, span=13, min_periods=1, adjust=True).mean()
    px['ema2'] = px['kurs_close'].ewm(ignore_na=False, span=34, min_periods=1, adjust=True).mean()
    px['ema3'] = px['kurs_close'].ewm(ignore_na=False, span=89, min_periods=1, adjust=True).mean()
    px['MACD'] = (px['ema1'] - px['ema2'])
    px['Signal_Line'] = px['MACD'].ewm(ignore_na=False, span=5, min_periods=1, adjust=True).mean()
    px['Signal_Line_Crossover'] = np.where(px['MACD'] > px['Signal_Line'], 1, 0)
    px['Signal_Line_Crossover'] = np.where(px['MACD'] < px['Signal_Line'], -1, px['Signal_Line_Crossover'])
    # px['Centerline_Crossover'] = np.where(px['MACD'] > 0, 1, 0)
    # px['Centerline_Crossover'] = np.where(px['MACD'] < 0, -1, px['Centerline_Crossover'])
    # px['Buy_Sell'] = (2 * (np.sign(px['Signal_Line_Crossover'] - px['Signal_Line_Crossover'].shift(1))))
    rolling1 = px['ema1']
    rolling2 = px['ema2']
    rolling3 = px['ema3']

    # lineare regression, erst vorbereiten
    reg = linear_model.LinearRegression()
    df['kurs_date2'] = pd.to_datetime(df['kurs_date'])
    df['date_delta'] = (df['kurs_date2'] - df['kurs_date2'].min()) / np.timedelta64(1, 'D')
    kurse = df['kurs_close']
    tage = df[['date_delta']]
    # dann besten fit suchen mit datenanfang der ersten halben daten
    letzter = len(kurse) - 1
    bestfit = 1
    bestscore = 0
    for i in range(1, letzter // 2):
        reg.fit(tage[i:letzter], kurse[i:letzter])
        thisscore = reg.score(tage[i:letzter], kurse[i:letzter])
        if (thisscore > bestscore):
            bestfit = i
            bestscore = thisscore
    trenddays = df['date_delta'][letzter] - df['date_delta'][bestfit]
    print("Trend days: " + str(trenddays))
    reg.fit(tage[bestfit:letzter], kurse[bestfit:letzter])
    # und dann erst vorhersage auf basis des besten fits
    df['kurs_predict'] = reg.predict(df[['date_delta']])
    aktie_deter = reg.score(tage[bestfit:letzter], kurse[bestfit:letzter])
    print("Bestimmtheit: " + str(aktie_deter))

    # stopkurs entnehmen aus unterstuetzunglinie am ende, underline ist 95 percentil der unterwerte
    df['kurs_diff'] = df['kurs_close'] - df['kurs_predict']
    df['kurs_diff'] = np.where(df['kurs_diff'] < 0, df['kurs_diff'], 0)
    underdist = np.percentile(df['kurs_diff'], 100 - 95)
    df['kurs_stop'] = df['kurs_predict'] + underdist
    underline = df['kurs_stop']
    prediction = df['kurs_predict']
    limitkurs = prediction[letzter]
    print("Limitkurs: " + str(limitkurs))
    stopkurs = underline[letzter]
    print("Stopkurs: " + str(stopkurs))

    # performance des betrachteten gewinn anteils hochgerechnet aufs jahr
    gewinn = kurse[letzter] - kurse[0]
    anteil = gewinn / kurse[0] * 100
    performance = anteil / df['date_delta'][letzter] * 365
    print("Performance: " + str(performance)+"% p.a.")

    # ergebnisse speichern der aktie
    cur = conn.cursor()
    try:
        print ("*** "+kurs_isin+" ***")
        cur.execute("update aktien set (aktien_perf,aktien_deter,aktien_stop,aktien_limit,aktien_trend) = (%s,%s,%s,%s,%s) where aktien_isin = %s;",(performance,aktie_deter,stopkurs,limitkurs,trenddays,kurs_isin))
    except:
        print (exception)
        sys.exit(1)
    cur.close()
    conn.commit()



# holt historische kurse aus frankfurt
def hole_aktien_kurse(aktien_isin,aktien_url,tage):
    global conn
    # dumme deutsche zellformate dolmetchen
    locale.setlocale(locale.LC_ALL, 'deu_deu')
    def cell2float(cell):
        if (cell!=""):
            ergebnis=locale.atof(cell)
        else:
            ergebnis=0.0
        return ergebnis
    # start robot instance
    driver = webdriver.Firefox()
    # tage berechnen
    d1 = datetime.today()
    d2 = datetime.today() - timedelta(days=tage)
    # kurse holen und parse html, bei tage=0 ohne extra parameter
    if tage>0:
        kurse_url = aktien_url.replace("/aktie/", "/aktie/kurshistorie/")+"/FSE/"+d1.strftime("%d.%m.%Y")+"_"+d2.strftime("%d.%m.%Y")+"#Kurshistorie"
    else:
        kurse_url = aktien_url.replace("/aktie/", "/aktie/kurshistorie/") + "/FSE#Kurshistorie"
    driver.get(kurse_url)
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")
    try:
        table = soup.find_all('table', attrs={'class': 'table'})[7]
        headings = [th.get_text() for th in table.find("tr").find_all("th")]
    except:
        headings=None
        pass
    if (headings!=None):
        cur = conn.cursor()
        for row in table.find_all("tr")[1:]:
            cols = [td.get_text() for td in row.find_all("td")]
            kurs_date = cols[0]
            if (kurs_date.count(".")==2):
                kurs_date = datetime.strptime(kurs_date, '%d.%m.%Y')
                kurs_date = kurs_date.strftime("%Y-%m-%d")
                kurs_open = cell2float(cols[1])
                kurs_close = cell2float(cols[2])
                kurs_high = cell2float(cols[3])
                kurs_low = cell2float(cols[4])
                kurs_sales = cell2float(cols[5])
                kurs_count = cell2float(cols[6])
                try:
                    command = "delete from kurse where kurs_date = '" + kurs_date + "' and kurs_isin = '" + aktien_isin + "';"
                    cur.execute(command)
                    cur.execute("insert into kurse (kurs_isin,kurs_date,kurs_open,kurs_close,kurs_high,kurs_low,kurs_sales,kurs_count) values (%s,%s,%s,%s,%s,%s,%s,%s);",(aktien_isin,kurs_date,kurs_open,kurs_close,kurs_high,kurs_low,kurs_sales,kurs_count))
                    print (aktien_isin+": "+kurs_date+" -> "+str(kurs_close))
                except:
                    print (exception)
                    sys.exit(1)
        cur.close()
        # und raus hier
        conn.commit()
    driver.close()



# erster durchlauf, holt fuer alle isin der aktien die aktien url
def hole_alle_aktien_urls():
    global conn
    cur = conn.cursor()
    cur.execute("SELECT aktien_isin, aktien_url from aktien where aktien_url is null;")
    rows = cur.fetchall()
    cur.close()
    for row in rows:
        hole_aktien_url(row[0],row[1])


# erster durchlauf, holt fuer alle isin der aktien alle historische kurse
def hole_alle_aktien_kurse():
    global conn
    cur = conn.cursor()
    cur.execute("""
        select f.anzahl, a.aktien_isin, a.aktien_url, f.letzter from 
            (select count(a.aktien_isin) as anzahl, max(k.kurs_date) as letzter ,a.aktien_isin 
            from aktien as a left join kurse as k 
            on (a.aktien_isin=k.kurs_isin) group by a.aktien_isin) 
        as f inner join aktien as a on (a.aktien_isin=f.aktien_isin)
        where letzter < CURRENT_TIMESTAMP - INTERVAL '7 days' or letzter is null
        order by anzahl asc;""")
    rows = cur.fetchall()
    cur.close()
    for row in rows:
        if (row[0]<200):
            hole_aktien_url(row[1],row[2])
            hole_aktien_kurse(row[1], row[2], 400)
        else:
            hole_aktien_kurse(row[1], row[2], 0)


# rating aller aktien
def bewerte_alle_aktien():
    global conn
    cur = conn.cursor()
    cur.execute("""
                select f.anzahl, a.aktien_isin, a.aktien_url, f.letzter from 
                    (select count(a.aktien_isin) as anzahl, max(k.kurs_date) as letzter ,a.aktien_isin 
                    from aktien as a left join kurse as k 
                    on (a.aktien_isin=k.kurs_isin) group by a.aktien_isin) 
                as f inner join aktien as a on (a.aktien_isin=f.aktien_isin)
                where anzahl>200;""")
    rows = cur.fetchall()
    cur.close()
    for row in rows:
        aktien_rating(row[1])



# hole realtime kurse aus frankfurt
def hole_realtime_kurse(aktien_isin):
    driver = webdriver.Firefox()
    driver.get(row[1])
    z = 0
    realtime_kurs = 0.0
    while ((z < 30) and not (realtime_kurs > 0.0)):
        time.sleep(1)
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        title1 = soup.title.text
        title2 = title1.strip().split(" ")
        if ("," in title2[1]):
            realtime_kurs = locale.atof(title2[1])
        else:
            realtime_kurs = 0.0
        z = z + 1
    # immer noch mehr als rolling?
    if (realtime_kurs > 0.0):
        if (realtime_kurs > row[2]):
            print("OK: " + str(realtime_kurs))
        else:
            print("FAIL: " + str(realtime_kurs))
    else:
        print("NO DATA")
    driver.close()
    return realtime_kurs



# bewerte das depot
def bewerte_depot():
    global conn
    locale.setlocale(locale.LC_ALL, 'deu_deu')
    cur = conn.cursor()
    cur.execute("""
                select d.depot_isin, a.aktien_url, a.aktien_stop from depot as d inner join aktien as a on (d.depot_isin=a.aktien_isin) order by d.id desc;
                """)
    rows = cur.fetchall()
    cur.close()
    for row in rows:
        # kurse aktualiseren und bewerten
        hole_aktien_kurse(row[0],row[1],0)
        aktien_rating(row[0])


# automate the boring stuff with python
def onvista_quick_order(aktien_order):
    # browser auf zum einloggen
    driver = webdriver.Firefox()
    driver.get("https://webtrading.onvista-bank.de/login")
    assert "onvista" in driver.title
    # und einloggen
    time.sleep(5)
    pyautogui.hotkey('ctrl', 'f')
    pyautogui.typewrite('ausblenden')
    time.sleep(1)
    pyautogui.hotkey('esc')
    time.sleep(1)
    pyautogui.hotkey('enter')
    time.sleep(2)
    pyautogui.hotkey('shift', 'tab')
    time.sleep(1)
    pyautogui.typewrite('***')
    time.sleep(1)
    pyautogui.hotkey('tab')
    time.sleep(1)
    pyautogui.typewrite('***')
    time.sleep(1)
    pyautogui.hotkey('enter')
    # und trading seite aufmachen zur quick order
    time.sleep(15)
    pyautogui.hotkey('ctrl', 'f')
    pyautogui.typewrite('Quick')
    time.sleep(1)
    pyautogui.hotkey('esc')
    time.sleep(1)
    pyautogui.hotkey('enter')
    time.sleep(1)
    pyautogui.hotkey('tab')
    pyautogui.hotkey('tab')
    pyautogui.hotkey('tab')
    pyautogui.hotkey('tab')
    # quick order absetzen
    pyautogui.typewrite(aktien_order)
    pyautogui.hotkey('tab')
    pyautogui.hotkey('tab')
    pyautogui.hotkey('enter')
    time.sleep(20)
    pyautogui.hotkey('tab')
    pyautogui.hotkey('tab')
    pyautogui.hotkey('tab')
    pyautogui.hotkey('enter')
    time.sleep(10)
    pyautogui.hotkey('tab')
    pyautogui.hotkey('tab')
    pyautogui.hotkey('enter')
    time.sleep(10)
    driver.close()



# setze alle stopkurse die abgelaufen sind neu
def setze_depot_stopkurse():
    global conn
    locale.setlocale(locale.LC_ALL, 'deu_deu')
    # stopkurs eine woche lang
    d2 = datetime.today() + timedelta(days=7)
    cur = conn.cursor()
    cur.execute("""
                select d.depot_isin, d.depot_anzahl, a.aktien_stop 
                from depot as d inner join aktien as a 
                on (d.depot_isin=a.aktien_isin) 
                where d.depot_stopdate < CURRENT_TIMESTAMP 
                order by d.id desc;
                """)
    rows = cur.fetchall()
    for row in rows:
        quickorder="V;EDF;"+row[0] + ";"+str(row[1])+";S;"+locale.format('%.2f', row[2])+";"+d2.strftime("%d%m%y")
        print ("Order: "+quickorder)
        cur.execute("update depot set (depot_stopdate,depot_stopkurs) = (%s,%s) where depot_isin=%s;",(d2,row[2],row[0]))
    cur.close()
    conn.commit()


# main
open_db()
#create_db()
#hole_xetra_aktien(110)
hole_alle_aktien_kurse()
bewerte_alle_aktien()
#bewerte_depot()
setze_depot_stopkurse()
#onvista_quick_order('K;EDE;DE0006916604;14;M')
#hole_aktien_kurse("US30303M1027","http://www.boerse-frankfurt.de/aktie/facebook-Aktie",500)
#aktien_rating('NL0011585146')
close_db()
