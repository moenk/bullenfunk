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
import tempfile
import configparser as ConfigParser
from twython import Twython
from matplotlib import pyplot
from sklearn import linear_model
from bs4 import BeautifulSoup
from selenium import webdriver
from datetime import datetime, timedelta


# datenbank erstellen
def create_db(conn):
    commands = (
        """ drop view if exists kandidaten;
        """,
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
        """,
        """ create view kandidaten 
            as select * from aktien 
                where aktien_deter>0.9
                and aktien_perf>0.0
            order by aktien_perf desc;
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
        sys.exit(1)


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


# kind of value function
def cell2float(cell):
    cell=cell.replace("-","")
    if (cell!=""):
        ergebnis=locale.atof(cell)
    else:
        ergebnis=0.0
    return ergebnis


# RSI bestimmen für kurse eines zeitraums
def rsi(prices):
    seed = np.diff(prices)
    up = seed[seed >= 0].sum() / len(prices)
    down = -seed[seed < 0].sum() / len(prices)
    return float((up) / (up + down))


# stratgie implementierung
def expert_advisor(px,day):
    bollfaktor = 2.0
    bolltage = 20
    buysell = 0
    px['ewm'] = px['kurs_close'].ewm(span=90).mean()
    px['bb0'] = px['kurs_close'].rolling(window=bolltage).mean()
    px['std'] = px['kurs_close'].rolling(window=bolltage).std()
    px['bb1'] = px['bb0'] + (px['std'] * bollfaktor)
    px['bb2'] = px['bb0'] - (px['std'] * bollfaktor)
    px['bbp'] = (px['kurs_close'] - px['bb2']) / (px['bb1'] - px['bb2'])
    rsiday=rsi(px['kurs_close'][day-13:day])
    # gerade preiswert? knappes kauf-fenster intraday
    #if (px['bbp'][day - 1] < 0.0) and (px['bbp'][day] > 0.0) and (px['bbp'][day] < 0.1) and (rsiday < 0.3):
    if (px['kurs_close'][day] > px['ewm'][day]):
    #if (rsiday < 0.3):
        buysell = 1
    # ziel erreicht! und raus damit
    #if (px['bbp'][day] > 0.45) or (rsiday > 0.7):
    if (px['kurs_close'][day] < px['ewm'][day]):
    #if (rsiday > 0.7):
        buysell = -1
    # bei fallendem kurs unter bollinger
    #if  (px['bbp'][day-1] > 0.0) and (px['bbp'][day] < 0.0):
    #    buysell = -1
    # am selben tag kein neukauf mehr da vergleich mit vortag
    return buysell


# holt alle realtimekurse von boerse online
def update_aktien_realtime_boerse(conn,url):
    print ("*** Hole Kurse von "+url+" ***")
    locale.setlocale(locale.LC_ALL, 'deu_deu')
    cur = conn.cursor()
    driver = webdriver.Firefox()
    #driver = webdriver.PhantomJS(executable_path="C:\Python34\PhantomJS.exe")
    try:
        driver.get(url)
        time.sleep(5)
    except:
        time.sleep(15)
        pass
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")
    for row in soup.find_all("tr"):
        aktien_isin=""
        cols = row.find_all("td")
        if (len(cols)==8):
            #for a in cols[0].find_all('a', title=True):
            #    aktien_url="http://www.boerse-online.de"+a['href']
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
    reg = linear_model.LinearRegression()
    df['kurs_date2'] = pd.to_datetime(df['kurs_date'])
    df['date_delta'] = (df['kurs_date2'] - df['kurs_date2'].min()) / np.timedelta64(1, 'D')
    kurse = df['kurs_close']
    tage = df[['date_delta']]
    # dann besten fit durch test suchen in 90% der ersten daten
    bestfit = 1
    bestscore = 0
    # teste erste 80% pareto verteilung trainingsmenge
    for i in range(1, (letzter // 5 * 4)):
        reg.fit(tage[i:letzter], kurse[i:letzter])
        thisscore = reg.score(tage[i:letzter], kurse[i:letzter])
        if (thisscore > bestscore):
            bestfit = i
            bestscore = thisscore
    trenddays = df['date_delta'][letzter] - df['date_delta'][bestfit]
    reg.fit(tage[bestfit:letzter], kurse[bestfit:letzter])
    # und dann erst vorhersage auf basis des besten fits
    df['kurs_predict'] = reg.predict(df[['date_delta']])
    aktie_deter = reg.score(tage[bestfit:letzter], kurse[bestfit:letzter])
    performance = reg.coef_[0]
    return (aktie_deter,performance,trenddays)


# komplette bewertung der aktie
def aktien_rating(conn,kurs_isin):
    # alle kursdaten holen
    zeitrahmen = 233
    command = "SELECT kurs_date, kurs_close from kurse as k where kurs_isin='" + kurs_isin + "' and kurs_date > NOW() - INTERVAL '" + str(
        zeitrahmen) + " days' order by kurs_date asc;"
    df = pd.read_sql(command, con=conn)
    # lineare regression, erst vorbereiten
    aktie_deter, performance, trenddays = optimierte_lineare_regression(df)
    # EA aufrufen
    letzter = len(df['kurs_close'])-1
    buysell=expert_advisor(df,letzter)
    # ergebnisse der aktie speichern
    cur = conn.cursor()
    print (kurs_isin, str(aktie_deter), str(performance), str(trenddays), buysell)
    cur.execute("update aktien set (aktien_perf,aktien_deter,aktien_buysell,aktien_days) = (%s,%s,%s,%s) where aktien_isin = %s;",(performance,aktie_deter,buysell,trenddays,kurs_isin))
    cur.close()
    conn.commit()


# holt historische kurse
def hole_historische_kurse_boerse(conn,aktien_isin):
    locale.setlocale(locale.LC_ALL, 'deu_deu')
    cur = conn.cursor()
    driver = webdriver.Firefox()
    driver.get("http://www.boerse-online.de/")
    el=driver.find_element_by_id("searchvalue")
    time.sleep(3)
    el.click()
    el.send_keys(aktien_isin)
    el.submit()
    time.sleep(8)
    el = driver.find_element_by_link_text("Historisch")
    el.click()
    time.sleep(2)
    el = driver.find_element_by_id('historic-prices-start-year')
    for option in el.find_elements_by_tag_name('option'):
        if option.text == '2014':
            option.click()
            break
    content = driver.find_element_by_id("request-historic-price")
    content.click()
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
    update_aktien_realtime_boerse(conn,'http://www.boerse-online.de/aktien/realtimekurse/DAX')


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
                where anzahl>100;""")
    rows = cur.fetchall()
    cur.close()
    for row in rows:
        aktien_rating(conn,row[1])


# alle historischen kurse holen wenn frischer titel in listen
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


# automate the boring stuff with python
def onvista_login():
    # hole login und passwort
    username=config.get('onvista', 'username')
    password=config.get('onvista', 'password')
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
    pyautogui.typewrite(username)
    time.sleep(1)
    pyautogui.hotkey('tab')
    time.sleep(1)
    pyautogui.typewrite(password)
    time.sleep(1)
    pyautogui.hotkey('enter')


# login und aktien kaufen im browser
def onvista_quick_order(aktien_order):
    # browser auf zum einloggen
    driver = webdriver.Firefox()
    driver.get("https://webtrading.onvista-bank.de/login")
    assert "onvista" in driver.title
    onvista_login()
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
    time.sleep(1)
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
    driver.quit()


# depot auf onvista aufmachen und db update
def onvista_depot_inventur():
    # browser auf zum einloggen
    driver = webdriver.Firefox()
    driver.get("https://webtrading.onvista-bank.de/login")
    assert "onvista" in driver.title
    onvista_login()
    # alle aufbauen lassen
    time.sleep(10)
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
    global conn
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


# kaufe aktien xetra market
def kaufe_aktien(aktien_isin,betrag):
    global conn
    locale.setlocale(locale.LC_ALL, 'deu_deu')
    cur = conn.cursor()
    cur.execute("select aktien_isin, aktien_ask from aktien where aktien_isin=%s;",(aktien_isin,))
    rows = cur.fetchall()
    row=rows[0]
    anzahl=math.floor(betrag/row[1])
    quickorder="K;EDE;"+row[0] + ";"+str(anzahl)+";M"
    print ("Order: "+quickorder)
    #onvista_quick_order(quickorder)
    cur.execute("delete from depot where depot_isin = %s;",(aktien_isin,))
    cur.execute("insert into depot (depot_isin,depot_anzahl) values (%s,%s);",(aktien_isin,anzahl))
    cur.close()
    conn.commit()


# strategie zur aktie pruefen und bei bedarf diagramm erstellen und tweeten
def backtest_expert(conn, aktien_isin, zeitrahmen, diagramm, zwitschern):
    print ("*** Starte Backtest: "+aktien_isin+" ***")
    global orderglobal
    global daysglobal
    command = "SELECT k.kurs_date, k.kurs_close, a.aktien_name, a.aktien_isin, a.aktien_url from kurse as k inner join aktien as a on (a.aktien_isin=k.kurs_isin) where k.kurs_isin='" + aktien_isin + "' and k.kurs_date > NOW() - INTERVAL '" + str(
        zeitrahmen) + " days' order by kurs_date asc;"
    df = pd.read_sql(command, con=conn)
    px = pd.DataFrame(df, columns=['kurs_date', 'kurs_close', 'aktien_name', 'aktien_isin', 'aktien_url'])
    #px['kurs_date'] = pd.to_datetime(px['kurs_date'])
    #px.index = px['kurs_date']
    #print(px)
    letzter = len(px.index)
    #print (letzter)
    # diagramm vorbereiten
    if diagramm:
        tage = range(0, letzter)
        kurse = px['kurs_close']
        plot_title =  str(px['aktien_name'][0]) + " - " + str(px['aktien_isin'][0])
        pyplot.title(plot_title)
        pyplot.plot(tage, kurse, color='black', linewidth=2)
        pyplot.xlabel('Tage seit ' + str(px['kurs_date'][0]))
        pyplot.ylabel('Kurs in Euro')
        pyplot.grid()
    # leeres depot und startkapital
    saldo = 0
    anzahl = 0
    order = 0
    bollfaktor = 2.0
    bolltage = 20
    for day in range(bolltage, letzter):
        #print("Tag: ", str(day), " - ", px['kurs_date'][day], "-> Kurs: ", str(px['kurs_close'][day]), "-> Saldo: ", str(saldo))
        buysell = expert_advisor(px,day)
        kurs_aktuell = px['kurs_close'][day]
        if (buysell == 1) and (anzahl == 0):
            #print("*** Kaufen: ", str(kurs_aktuell))
            saldo = saldo - kurs_aktuell
            anzahl = 1
            order = order + 1
            if diagramm:
                pyplot.plot(day, kurs_aktuell, "go")
        if (buysell == -1) and (anzahl == 1):
            #print("*** Verkaufen: ", str(kurs_aktuell))
            saldo = saldo + kurs_aktuell
            anzahl = 0
            order = order + 1
            if diagramm:
                pyplot.plot(day, kurs_aktuell, "ro")
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
    if diagramm:
        px['bb0'] = px['kurs_close'].rolling(window=bolltage).mean()
        px['std'] = px['kurs_close'].rolling(window=bolltage).std()
        bollinger0 = px['bb0']
        pyplot.plot(tage, bollinger0, color='darkgray', linewidth=1, linestyle="dotted")
        bollinger1 = px['bb0'] + (px['std'] * bollfaktor)
        bollinger2 = px['bb0'] - (px['std'] * bollfaktor)
        pyplot.fill_between(tage, bollinger1, bollinger2, color="lightyellow")
        pyplot.plot(tage, bollinger1, color='darkgray', linewidth=1)
        pyplot.plot(tage, bollinger2, color='darkgray', linewidth=1)
        sma90 = px['kurs_close'].rolling(window=90).mean()
        pyplot.plot(tage, sma90, color='darkblue', linewidth=1)
    if zwitschern:
        akten_chart = str(tempfile.gettempdir()) + "/" + str(px['aktien_isin'][0] + ".PNG")
        print(akten_chart)
        pyplot.savefig(akten_chart, format="png")
        twitter = Twython(config.get('twitter', 'APP_KEY'), config.get('twitter', 'APP_SECRET'), config.get('twitter', 'OAUTH_TOKEN'), config.get('twitter', 'OAUTH_TOKEN_SECRET'))
        photo = open(akten_chart, 'rb')
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
def backtest_alle_kandidaten(conn,zeitrahmen):
    kandidaten=0
    gewinne=0
    verluste=0
    gewinner=0
    verlierer=0
    cur = conn.cursor()
    cur.execute("""
                select f.anzahl, a.aktien_isin, a.aktien_url, f.letzter from 
                    (select count(a.aktien_isin) as anzahl, max(k.kurs_date) as letzter ,a.aktien_isin 
                    from kandidaten as a left join kurse as k 
                    on (a.aktien_isin=k.kurs_isin) group by a.aktien_isin) 
                as f inner join aktien as a on (a.aktien_isin=f.aktien_isin)
                where anzahl>%s;""",(zeitrahmen,))
    rows = cur.fetchall()
    for row in rows:
        prozent=backtest_expert(conn, row[1], zeitrahmen, False, False)
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
            # onvista_quick_order(quickorder)
            cur.execute("delete from depot where depot_isin=%s;",(row[0],))
            backtest_expert(row[0], 200, True, True)
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
    print ("*** Starte Einkaufprogramm ***")
    cur = conn.cursor()
    cur.execute("""
        select aktien_isin, aktien_name from kandidaten 
        where aktien_buysell=1
        order by aktien_deter desc;
    """)
    rows = cur.fetchall()
    for row in rows:
        print ("Kaufsignal: "+row[0]+" -> "+row[1])
        if (aktien_isin_im_depot(row[0])==0) and (positionen_im_depot()<5):
            kaufe_aktien(row[0], 2000)
            backtest_expert(row[0], 200, True, True)


def aktuelle_stunde():
    return int(time.strftime("%H"))
def aktueller_wochentag():
    return int(time.strftime("%w"))
def xetra_trading_bot(conn):
    print ("*** Starte XETRA-Trading-Bot wenn alleine zur Handelszeit ***")
    while ((aktuelle_stunde() >= 9) and (aktuelle_stunde() < 17) and (aktueller_wochentag() >= 1) and (aktueller_wochentag() <= 5)) and (allein_zu_haus()==True):
        try:
            hole_alle_aktien_kurse(conn)
            hole_alle_historischen_kurse(conn)
            bewerte_alle_aktien(conn)
            depot_verkauf(conn)
            depot_einkauf(conn)
        except:
            pass
        print("*** Wartepause ***")
        time.sleep(300)
    print ("*** XETRA-Trading-Bot beendet ***")



# main
config = ConfigParser.RawConfigParser()
config.read(os.path.expanduser("~/bullenfunk.ini"))
conn = psycopg2.connect("dbname='bullenfunk' user='"+config.get('postgresql', 'pguser')+"' host='localhost' password='"+config.get('postgresql', 'pgpass')+"'")
#
#create_db(conn)
#hole_alle_aktien_kurse(conn)
#hole_alle_historischen_kurse(conn)
bewerte_alle_aktien(conn)
backtest_alle_kandidaten(conn,200)
#
#depot_verkauf()
#depot_einkauf()
#onvista_depot_inventur()
#print (positionen_im_depot())
#backtest_expert("DE000PSM7770",200,True,False)
#
#
#
#xetra_trading_bot()
#
#
#
conn.close()
