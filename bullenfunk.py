#
# file: bullenfunk.py
#
# coder: moenk
#
# required: selenium bindings and geckodriver for firefox
#           postgresql with postgis
#           twitter auth data
#

import sys
import math
import time
import os.path
import locale
import psycopg2
import pyautogui
import subprocess
import pandas as pd
import numpy as np
import holidays
import tempfile
from tqdm import tqdm
import configparser as ConfigParser
from twython import Twython
from matplotlib import pyplot
from sklearn import linear_model
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.keys import Keys



# datenbank erstellen
def create_db(conn):
    commands = (
        """ drop table if exists kurse;
        """,
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
        """ drop table if exists aktien;
        """,
        """ CREATE TABLE aktien (
            id SERIAL PRIMARY KEY,
            aktien_isin VARCHAR(25),
            aktien_name VARCHAR(250),
            aktien_url VARCHAR(250),
            aktien_bid FLOAT,
            aktien_ask FLOAT,
            aktien_perf FLOAT,
            aktien_deter FLOAT,
            aktien_buysell INTEGER,
            aktien_days INTEGER,
            aktien_backtest FLOAT
        );
        """,
        """ drop table if exists depot;
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
    cur = conn.cursor()
    # create table one by one
    for command in commands:
        print(command)
        cur.execute(command)
    # close communication with the PostgreSQL database server
    cur.close()
    # commit the changes
    try:
        conn.commit()
    except:
        print (exception)
        exit(1)


# kind of value function
def cell2float(cell):
    cell=cell.replace("-","")
    if (cell!=""):
        ergebnis=locale.atof(cell)
    else:
        ergebnis=0.0
    return ergebnis


# gucken ob die seite fertig ist
def page_has_loaded(driver):
    page_state = driver.execute_script('return document.readyState;')
    return page_state == 'complete'


# RSI bestimmen für kurse eines zeitraums
def rsi(prices):
    seed = np.diff(prices)
    up = seed[seed >= 0].sum() / len(prices)
    down = -seed[seed < 0].sum() / len(prices)
    return float((up) / (up + down))


# stratgie implementierung
def expert_advisor(px,deter,performance,vorhersage):
    buysell = 0
    # kaufsignal
    if (performance > 0.02) and (deter > 0.90):
        buysell = 1
    # kurs nicht mehr bullish, raus damit!
    if (performance < 0.00):
        buysell = -1
    # das wars schon
    return buysell


# holt alle realtimekurse von boerse online
def update_aktien_realtime_boerse(conn,url):
    print ("*** Hole Kurse von "+url+" ***")
    locale.setlocale(locale.LC_ALL, 'deu_deu')
    cur = conn.cursor()
    driver = webdriver.Chrome('c:\\Python36\chromedriver.exe')
    driver.get(url)
    while not(page_has_loaded(driver)):
        time.sleep(1)
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")
    for row in soup.find_all("tr"):
        aktien_isin=""
        cols = row.find_all("td")
        if (len(cols)==8):
            aktien_index=url[49:]
            aktien_name = cols[0].text.strip()
            aktien_isin = cols[1].text.strip()
            aktien_bid = cell2float(cols[3].text)
            aktien_ask = cell2float(cols[4].text)
            if (aktien_ask==0.0):
                aktien_ask=cell2float(cols[2].text)
            kurs_datum = time.strftime("%Y-%m-%d")
            kurs_close = np.mean([aktien_bid, aktien_ask])
            kurs_vortag = cell2float(cols[2].text)
            yesterday = datetime.now() - timedelta(days=1)
            date_gestern = yesterday.strftime("%Y-%m-%d")
        # eintragen wenn ISIN gueltig und kurs auch
        if (aktien_isin!="") and (aktien_bid>0.0):
            print(aktien_isin, aktien_name, aktien_index, aktien_bid, aktien_ask, kurs_datum)
            cur.execute("delete from aktien where aktien_isin=%s;",(aktien_isin,))
            cur.execute("insert into aktien (aktien_isin,aktien_name,aktien_url,aktien_bid,aktien_ask) values (%s,%s,%s,%s,%s);",(aktien_isin,aktien_name,aktien_index,aktien_bid,aktien_ask))
            cur.execute("delete from kurse where kurs_isin=%s and kurs_date=%s;",(aktien_isin,kurs_datum))
            cur.execute("insert into kurse (kurs_isin,kurs_date,kurs_close) values (%s,%s,%s);",(aktien_isin,kurs_datum,kurs_close))
            cur.execute("delete from kurse where kurs_isin=%s and kurs_date=%s;",(aktien_isin,date_gestern))
            cur.execute("insert into kurse (kurs_isin,kurs_date,kurs_close) values (%s,%s,%s);",(aktien_isin,date_gestern,kurs_vortag))
    # und feierabend
    cur.close()
    conn.commit()
    driver.close()


def optimierte_lineare_regression(df):
    letzter = len(df['kurs_close'])-1
    reg = linear_model.LinearRegression(n_jobs=1)
    #df['kurs_date2'] = pd.to_datetime(df['kurs_date'])
    #df['date_delta'] = (df['kurs_date2'] - df['kurs_date2'].min()) / np.timedelta64(1, 'D')
    #tage = df[['date_delta']]
    df['tage'] = range(0,letzter+1)
    kurse = df['kurs_close']
    tage=df[['tage']]
    # dann besten fit durch test suchen in ersten 80% der daten
    df['score']=0
    for i in range(0, (letzter // 5 * 4)):
        reg.fit(tage[i:letzter], kurse[i:letzter])
        df.loc[i,'score']=reg.score(tage[i:letzter], kurse[i:letzter])
    df['scoremean']=df['score'].rolling(window=3,center=True).mean()
    bestfit=df['scoremean'].idxmax()
    #bestscore=df['scoremean'][bestfit]
    #print (bestfit,bestscore)
    #trenddays = df['date_delta'][letzter] - df['date_delta'][bestfit]
    trenddays=letzter-bestfit
    reg.fit(tage[bestfit:letzter], kurse[bestfit:letzter])
    # und dann erst vorhersage auf basis des besten fits
    #df['kurs_predict'] = reg.predict(df[['date_delta']])
    df['kurs_predict'] = reg.predict(tage)
    aktie_deter = df['score'][bestfit] #reg.score(tage[bestfit:letzter], kurse[bestfit:letzter])
    performance = reg.coef_[0]
    vorhersage=df['kurs_predict']
    #residuen=kurse[bestfit:letzter]-vorhersage[bestfit:letzter]
    #vorhersage=vorhersage+residuen.min() # plus negative werte
    return (aktie_deter,performance,trenddays,vorhersage)


# komplette bewertung der aktie
def aktien_rating(conn,kurs_isin):
    # alle kursdaten holen, schon aufsteigend sortiert
    zeitrahmen = 233
    command = """SELECT k.kurs_date, k.kurs_close from kurse as k 
                 where kurs_isin='""" + kurs_isin + """' and kurs_date > NOW() - INTERVAL '""" + str(zeitrahmen) + """ days' 
                 order by kurs_date asc;"""
    df = pd.read_sql(command, con=conn)
    aktie_deter, performance, trenddays, vorhersage = optimierte_lineare_regression(df)
    # EA aufrufen
    letzter = len(df['kurs_close'])-1
    buysell=expert_advisor(df,aktie_deter,performance, vorhersage)
    # ergebnisse der aktie speichern
    cur = conn.cursor()
    print (kurs_isin, str(aktie_deter), str(performance), str(trenddays), buysell, vorhersage[letzter])
    cur.execute("update aktien set (aktien_perf,aktien_deter,aktien_buysell,aktien_days) = (%s,%s,%s,%s) where aktien_isin = %s;",(performance,aktie_deter,buysell,trenddays,kurs_isin))
    cur.close()
    conn.commit()


# holt historische kurse
def hole_historische_kurse_boerse(conn,aktien_isin):
    locale.setlocale(locale.LC_ALL, 'deu_deu')
    cur = conn.cursor()
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    driver = webdriver.Chrome('c:\\Python36\chromedriver.exe',chrome_options=options)
    driver.get("http://www.boerse-online.de/")
    el=driver.find_element_by_id("searchvalue")
    time.sleep(3)
    el.click()
    el.send_keys(aktien_isin)
    el.submit()
    time.sleep(5)
    while not(page_has_loaded(driver)):
        time.sleep(1)
    el = driver.find_element_by_link_text("Historisch")
    el.click()
    time.sleep(2)
    el = driver.find_element_by_id('historic-prices-start-year')
    for option in el.find_elements_by_tag_name('option'):
        if option.text == '2014':
            option.click()
            break
    el = driver.find_element_by_id("request-historic-price")
    el.click()
    time.sleep(5)
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")
    for row in soup.find_all("tr"):
        cols = [td.get_text() for td in row.find_all("td")]
        if (len(cols)==6):
            kurs_date = cols[0]
            if (kurs_date.count(".") == 2):
                kurs_date = datetime.strptime(kurs_date, '%d.%m.%Y')
                kurs_date = kurs_date.strftime("%Y-%m-%d")
                kurs_open = cell2float(cols[1])
                kurs_close = cell2float(cols[2])
                kurs_high = cell2float(cols[3])
                kurs_low = cell2float(cols[4])
                kurs_count = cell2float(cols[5])
                cur.execute("delete from kurse where kurs_date = %s and kurs_isin = %s;",(kurs_date,aktien_isin))
                cur.execute("insert into kurse (kurs_isin,kurs_date,kurs_open,kurs_close,kurs_high,kurs_low,kurs_count) values (%s,%s,%s,%s,%s,%s,%s);",(aktien_isin, kurs_date, kurs_open, kurs_close, kurs_high, kurs_low, kurs_count))
                print(aktien_isin,kurs_close,kurs_date)
    cur.close()
    conn.commit()
    driver.quit()


# holt die aktuellen kurse aus alle aktien listen
def hole_alle_aktien_kurse(conn):
    update_aktien_realtime_boerse(conn,"http://www.boerse-online.de/aktien/realtimekurse/Dow_Jones")
    update_aktien_realtime_boerse(conn,"http://www.boerse-online.de/aktien/realtimekurse/Euro_Stoxx_50")
    update_aktien_realtime_boerse(conn,"http://www.boerse-online.de/aktien/realtimekurse/TecDAX")
    update_aktien_realtime_boerse(conn,"http://www.boerse-online.de/aktien/realtimekurse/SDAX")
    update_aktien_realtime_boerse(conn,"http://www.boerse-online.de/aktien/realtimekurse/MDAX")
    update_aktien_realtime_boerse(conn,"http://www.boerse-online.de/aktien/realtimekurse/DAX")


# rating aller aktien
def bewerte_alle_aktien(conn):
    print ("*** Lineare Regression und Bewertung ***")
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
        aktien_rating(conn,row[1])


# alle historischen kurse holen wenn frischer titel in listen, dann gibt nur kurs von heute und gestern
def hole_alle_historischen_kurse(conn):
    cur = conn.cursor()
    cur.execute("select * from (select kurs_isin, count(*) as anzahl from kurse group by kurs_isin) as k where anzahl=2;")
    rows = cur.fetchall()
    cur.close()
    for row in rows:
        try:
            hole_historische_kurse_boerse(conn,row[0])
        except:
            pass


# automate login
def onvista_login_desktop(driver):
    # hole login und passwort
    username=config.get('onvista', 'username')
    password=config.get('onvista', 'password')
    # und einloggen
    time.sleep(3)
    el = driver.find_element_by_link_text("Sicherheitstastatur ausblenden")
    el.send_keys(Keys.RETURN)
    time.sleep(3)
    el= driver.find_element_by_name('login')
    el.send_keys(username)
    el= driver.find_element_by_name('password')
    el.send_keys(password)
    time.sleep(3)
    el = driver.find_element_by_id('performLoginButton')
    el.send_keys(Keys.RETURN)


def wait_for_xpath_element(driver,xpathstr):
    fertig=False
    versuch=0
    while not(fertig) and (versuch<100):
        try:
            el=driver.find_element_by_xpath(xpathstr)
            fertig=True
        except:
            versuch=versuch+1
            if (versuch>60):
                raise Exception('Timeout waiting for: '+xpathstr)
            fertig=False
        time.sleep(1)
    return (el)


# login und aktien kaufen im browser
def onvista_quick_order(aktien_order):
    # browser auf zum einloggen
    driver = webdriver.Chrome('c:\\Python36\chromedriver.exe')
    driver.get("https://webtrading.onvista-bank.de/login")
    assert "onvista" in driver.title
    onvista_login_desktop(driver)
    # alles aufbauen lassen
    time.sleep(3)
    while not(page_has_loaded(driver)):
        time.sleep(1)
    # und ab damit
    el = wait_for_xpath_element(driver,"//*[@placeholder='Bitte geben Sie hier Ihre Quick-Order ein.']")
    el.send_keys(aktien_order)
    time.sleep(3)
    el = driver.find_element_by_xpath("//*[@class='btn btn-primary btn-sm pull-right']")
    el.send_keys(Keys.RETURN)
    # abwarten sicherheitsabfrage
    time.sleep(3)
    while not(page_has_loaded(driver)):
        time.sleep(1)
    el = wait_for_xpath_element(driver,"//*[@class='btn btn-primary btn-sm']")
    el.send_keys(Keys.RETURN)
    time.sleep(10)
    driver.quit()


# depot auf onvista aufmachen und db update
def onvista_depot_inventur(conn):
    # browser auf zum einloggen
    driver = webdriver.Chrome('c:\\Python36\chromedriver.exe')
    driver.get("https://webtrading.onvista-bank.de/login")
    assert "onvista" in driver.title
    onvista_login_desktop(driver)
    # alles aufbauen lassen
    while not(page_has_loaded(driver)):
        time.sleep(1)
    # und los gehts
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"class": "table-full table-striped"})
    def finde_isin_in_tabelle(aktien_isin,table):
        gefunden=False
        for row in table.findAll("tr"):
            zeile=(row.text)
            posiition = zeile.find(aktien_isin)
            if (posiition>0):
                gefunden=True
        return gefunden
    # entferne nicht mehr vorhanndene aus DB
    cur = conn.cursor()
    cur.execute("select d.depot_isin from depot as d order by d.id desc;")
    rows = cur.fetchall()
    for row in rows:
        if (finde_isin_in_tabelle(row[0],table)):
            print("Gefunden: "+row[0])
        else:
            print("Verkauft: "+row[0])
            cur.execute("delete from depot where depot_isin = %s;", (row[0],))
    cur.close()
    conn.commit()
    time.sleep(10)
    driver.quit()


# strategie zur aktie pruefen und bei bedarf diagramm erstellen und tweeten
def backtest_expert(conn, aktien_isin, zeitrahmen, diagramm, zwitschern):
    print ("*** Starte Backtest: "+aktien_isin+" ***")
    global orderglobal
    global daysglobal
    # leeres depot und startkapital
    saldo = 0
    anzahl = 0
    order = 0
    # doppelte anzahl kurse aus der DB holen
    command = """select kurs_date, kurs_close, aktien_name, aktien_isin, aktien_url from (
                    select distinct kurs_date, kurs_close, kurs_isin 
                    from kurse as k
                    where kurs_isin='""" + aktien_isin + """' 
                    order by kurs_date desc limit """ + str(zeitrahmen*2) + """ ) as foo 
                inner join aktien as a
                on (foo.kurs_isin=a.aktien_isin)
                order by kurs_date asc;"""
    df2 = pd.read_sql(command, con=conn)
    tage=range(1, zeitrahmen)
    for day in tqdm(tage, dynamic_ncols=True, desc="Days"):
        # fenster verschieben aus der doppelten DB tabelle
        offset=day-1
        # und dataframe für pandas mit neuen index draus ziehen
        px = pd.DataFrame(df2[offset:zeitrahmen+offset], columns=['kurs_date', 'kurs_close', 'aktien_name', 'aktien_isin', 'aktien_url'])
        px.index = range(len(px))
        # letzter ist immer der tag zu dem in diesem loop gestetet wird
        letzter = len(px.index)-1
        #print("Tag: ", str(day), " - ", px['kurs_date'][letzter], "-> Kurs: ", str(px['kurs_close'][letzter]), "-> Saldo: ", str(saldo))
        # neue lin reg für jeden tag im backtest
        aktie_deter, performance, trenddays, vorhersage = optimierte_lineare_regression(px)
        #print (aktie_deter,performance,trenddays)
        #print (stopkurs)
        buysell = expert_advisor(px,aktie_deter,performance,vorhersage)
        kurs_aktuell = px['kurs_close'][letzter] # day
        if (buysell == 1) and (anzahl == 0):
            #print("*** Kaufen: ", str(kurs_aktuell))
            saldo = saldo - kurs_aktuell
            anzahl = 1
            order = order + 1
            if diagramm:
                pyplot.plot(day, kurs_aktuell, "go",  markersize=12)
        if (buysell == -1) and (anzahl == 1):
            #print("*** Verkaufen: ", str(kurs_aktuell))
            saldo = saldo + kurs_aktuell
            anzahl = 0
            order = order + 1
            if diagramm:
                pyplot.plot(day, kurs_aktuell, "ro",  markersize=12)
        if (anzahl == 1):
            daysglobal = daysglobal + 1
    # depot am ende ausleeren
    if (anzahl == 1):
        #print("Verkaufen: ", str(kurs_aktuell))
        saldo = saldo + kurs_aktuell
    print("Saldo: " + str(saldo))
    print("Order= " + str(order))
    orderglobal=orderglobal+order
    prozent = saldo / px['kurs_close'].mean() *100
    print("Performance: " + str(prozent) + "%")
    if diagramm or zwitschern:
        kurse = px['kurs_close']
        tage = range(0, len(kurse))
        plot_title =  str(px['aktien_name'][letzter]) + " - " + str(px['aktien_isin'][letzter])
        pyplot.title(plot_title)
        pyplot.plot(tage, kurse, color='black', linewidth=2)
        #sma90 = px['kurs_close'].rolling(window=90).mean()
        #pyplot.plot(tage,sma90, color='blue', linewidth=1)
        pyplot.plot(tage[int(letzter-trenddays):int(letzter)], vorhersage[int(letzter-trenddays):int(letzter)], color='purple', linewidth=2)
        pyplot.xlabel('Tage seit ' + str(px['kurs_date'][0]))
        pyplot.ylabel('Kurs in Euro')
        pyplot.grid()
    if diagramm:
        bolltage=20
        bollfaktor=2.0
        px['bb0'] = px['kurs_close'].rolling(window=bolltage).mean()
        px['std'] = px['kurs_close'].rolling(window=bolltage).std()
        bollinger0 = px['bb0']
        pyplot.plot(tage, bollinger0, color='darkgray', linewidth=1, linestyle="dotted")
        bollinger1 = px['bb0'] + (px['std'] * bollfaktor)
        bollinger2 = px['bb0'] - (px['std'] * bollfaktor)
        pyplot.fill_between(tage, bollinger1, bollinger2, color="lightyellow")
        pyplot.plot(tage, bollinger1, color='darkgray', linewidth=1)
        pyplot.plot(tage, bollinger2, color='darkgray', linewidth=1)
    if zwitschern:
        aktien_chart = str(tempfile.gettempdir()) + "/" + str(px['aktien_isin'][0] + ".PNG")
        print(aktien_chart)
        pyplot.savefig(aktien_chart, format="png")
        twitter = Twython(config.get('twitter', 'APP_KEY'), config.get('twitter', 'APP_SECRET'), config.get('twitter', 'OAUTH_TOKEN'), config.get('twitter', 'OAUTH_TOKEN_SECRET'))
        photo = open(aktien_chart, 'rb')
        response = twitter.upload_media(media=photo)
        tweet_title = str(px['aktien_name'][0]) + " - #" + str(px['aktien_isin'][0]) + " #"+px['aktien_name'][0].split(' ', 1)[0] + " #" + str(px['aktien_url'][0])
        twitter.update_status(status=tweet_title, media_ids=[response['media_id']])
    else:
        pyplot.show()
    pyplot.clf()
    pyplot.cla()
    pyplot.close()
    return prozent


# alle kandiaten mit genug kursen backtesten und bilder erzeugen
orderglobal=0
daysglobal=0
def backtest_aktuelle_kandidaten(conn,zeitrahmen,showgraph):
    kandidaten=0
    gewinne=0
    verluste=0
    gewinner=0
    verlierer=0
    cur = conn.cursor()
    cur.execute("""
                select f.anzahl, a.aktien_isin, a.aktien_url, f.letzter from 
                    (select count(a.aktien_isin) as anzahl, max(k.kurs_date) as letzter ,a.aktien_isin 
                    from aktien as a left join kurse as k 
                    on (a.aktien_isin=k.kurs_isin) group by a.aktien_isin) 
                as f inner join aktien as a on (a.aktien_isin=f.aktien_isin)
                where a.aktien_buysell=1 and anzahl>%s 
                order by aktien_perf desc;""",(zeitrahmen,))
    rows = cur.fetchall()
    for row in rows:
        prozent=backtest_expert(conn, row[1], zeitrahmen, showgraph, False)
        # zur verwendung bei kaufentscheidung merken
        cur.execute("update aktien set aktien_backtest=%s where aktien_isin=%s", (prozent, row[1],))
        if prozent > 0:
            gewinne=gewinne+prozent
            gewinner=gewinner+1
        if prozent < 0:
            verluste=verluste+prozent
            verlierer=verlierer+1
        kandidaten=kandidaten+1
    print ("Kandidaten: "+str(kandidaten))
    print ("Gewinne: "+str(gewinne))
    print ("Gewinner: "+str(gewinner))
    print ("Verluste: "+str(verluste))
    print ("Verlierer: "+str(verlierer))
    print ("Ordergesamt: "+str(orderglobal))
    print ("Tage: "+str(daysglobal))
    cur.close()
    conn.commit()


# kaufe aktien xetra market
def kaufe_aktien(conn,aktien_isin,betrag):
    locale.setlocale(locale.LC_ALL, 'deu_deu')
    cur = conn.cursor()
    cur.execute("select aktien_isin, aktien_ask from aktien where aktien_isin=%s;",(aktien_isin,))
    rows = cur.fetchall()
    row=rows[0]
    anzahl=math.floor(betrag/row[1])
    quickorder="K;EDE;"+row[0] + ";"+str(anzahl)+";M"
    print ("Order: "+quickorder)
    onvista_quick_order(quickorder)
    cur.execute("delete from depot where depot_isin = %s;",(aktien_isin,))
    cur.execute("insert into depot (depot_isin,depot_anzahl) values (%s,%s);",(aktien_isin,anzahl))
    cur.close()
    conn.commit()


# pruefe alle papiere im depot auf buysell
def depot_verkauf(conn):
    cur = conn.cursor()
    cur.execute("""
                select d.depot_isin, d.depot_anzahl, a.aktien_buysell, a.aktien_bid
                from depot as d inner join aktien as a 
                on (d.depot_isin=a.aktien_isin) 
                order by d.id desc;
                """)
    rows = cur.fetchall()
    print ("*** Starte Verkaufsprogramm ***")
    for row in rows:
        if (row[2]<0): # buysell -1 ?
            print("Verkaufen: "+row[0])
            quickorder = "V;EDE;" + row[0] + ";" + str(row[1]) + ";M"
            print("Order: " + quickorder)
            onvista_quick_order(quickorder)
            cur.execute("delete from depot where depot_isin=%s;",(row[0],))
            backtest_expert(conn, row[0], 200, True, True)
        else:
            print(row[0] + ": " + str(row[1]) + " Aktien halten, Kurs: "+ str(row[3]))
    cur.close()
    conn.commit()


# guckt nach wieviele aktien wir schon haben von dieser isin
def aktien_isin_im_depot(conn,aktien_isin):
    cur = conn.cursor()
    cur.execute("select depot_anzahl from depot where depot_isin=%s;",(aktien_isin,))
    row = cur.fetchone()
    if row is None:
        aktien_anzahl = 0
    else:
        aktien_anzahl = row[0]
    return aktien_anzahl
    cur.close


# guckt nach wieviele positionen wir im depot haben
def positionen_im_depot(conn):
    cur = conn.cursor()
    cur.execute("select count(depot_isin) as anzahl from depot;")
    row = cur.fetchone()
    aktien_anzahl = row[0]
    return aktien_anzahl
    cur.close


# kaufe kandidaten nach backtest und zwitschere
def depot_einkauf(conn):
    cur = conn.cursor()
    cur.execute("select aktien_isin, aktien_name, aktien_deter from aktien where aktien_buysell=1 order by aktien_deter desc;")
    print("*** Starte Einkaufprogramm ***")
    rows = cur.fetchall()
    for row in rows:
        print ("Kaufsignal: "+row[0]+" -> "+row[1])
        if (aktien_isin_im_depot(conn,row[0])==0) and (positionen_im_depot(conn)<5):
            kaufe_aktien(conn, row[0], 2000)
            backtest_expert(conn, row[0], 200, True, True)


# zeige einen plot der korrelation zu backtest und bestimmtheit
def backtest_korrelation_scatter(conn):
    cursor = conn.cursor()
    cursor.execute('select aktien_backtest, aktien_deter, aktien_perf, aktien_days from aktien where aktien_buysell=1');
    rows = cursor.fetchall()
    df = pd.DataFrame([[ij for ij in i] for i in rows])
    print (df)
    x=df[1]
    y=df[0]
    pyplot.scatter(x, y)
    pyplot.show()


# sind wir alleine hier oder geht schon was?
def allein_zu_haus():
    scount=0
    s = subprocess.check_output('tasklist', shell=True)
    for s3 in str(s).split(" "):
        if ("python" in s3):
            scount=scount+1
    allein=False
    if scount==1:
        allein=True
    return allein


# sind wir dran jetzt?
def ist_jetzt_handelszeit():
    feiertag=datetime.now().strftime('%Y-%m-%d') in holidays.DE()
    aktuelle_stunde=int(time.strftime("%H"))
    aktueller_wochentag=int(time.strftime("%w"))
    richtige_zeit=((aktuelle_stunde >= 9) and (aktuelle_stunde < 17))
    richtiger_tag=((aktueller_wochentag >= 1) and (aktueller_wochentag <= 5))
    return (not(feiertag) and richtiger_tag and richtige_zeit)


# lasset die spiele beginnen!
def xetra_trading_bot(conn):
    print ("*** Starte XETRA-Trading-Bot wenn alleine zur Handelszeit ***")
    while (ist_jetzt_handelszeit()):
        try:
            hole_alle_aktien_kurse(conn)
            hole_alle_historischen_kurse(conn)
            bewerte_alle_aktien(conn)
            depot_verkauf(conn)
            depot_einkauf(conn)
        except:
            pass
        print("*** Wartepause ***")
        for i in tqdm(range(1,15),dynamic_ncols=True, desc="Minuten"):
            time.sleep(60)
    print ("*** XETRA-Trading-Bot beendet ***")



# main
if not(allein_zu_haus()):
    exit(1)
config = ConfigParser.RawConfigParser()
config.read(os.path.expanduser("~/bullenfunk.ini"))
conn = psycopg2.connect("dbname='bullenfunk' user='"+config.get('postgresql', 'pguser')+"' host='localhost' password='"+config.get('postgresql', 'pgpass')+"'")
#
#create_db(conn)
#hole_alle_aktien_kurse(conn)
#hole_alle_historischen_kurse(conn)
#bewerte_alle_aktien(conn)
#onvista_depot_inventur(conn)
#depot_verkauf(conn)
#depot_einkauf(conn)
#print (positionen_im_depot(conn))
#print (aktien_isin_im_depot(conn,'DE0005140008'))
#onvista_depot_inventur(conn)
#kaufe_aktien(conn,"DE0005232805",2000)
#backtest_aktuelle_kandidaten(conn,50,False)
#backtest_expert(conn,"DE0005232805",200,True,False)
#backtest_korrelation_scatter(conn)
#
xetra_trading_bot(conn)
#
conn.close()
