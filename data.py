# from create_dbs import *
import requests
from bs4 import BeautifulSoup
import json
from requests_oauthlib import OAuth2Session
import secrets
import sqlite3
import plotly.plotly as py
import plotly.graph_objs as go
import numpy as np

GOOGLE_CACHE_FILE = "google_info.json"
YELP_CACHE_FILE = "yelp_info.json"
DBNAME = 'ratings.db'
GOOGLE_TBL = 'Google'
YELP_TBL = 'Yelp'


def get_google_data(city, state, keyword):
    place = str(city + ", " + state)
    keyword = keyword
    key = str(place + ", " +  keyword)


    cache_try = get_cached_data(key, GOOGLE_CACHE_FILE)

    if cache_try == None:
        # print("requesting new data")
        G1_baseurl = "https://maps.googleapis.com/maps/api/place/textsearch/json"
        parameters = {'key':secrets.google_places_key, 'query' : place}
        resp1 = requests.get(G1_baseurl, parameters).text
        location = json.loads(resp1)
        Lat = (location['results'][0]['geometry']['location']['lat'])
        Long = (location['results'][0]['geometry']['location']['lng'])
        location = str(Lat) + "," + str(Long)


        G2_baseurl = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
        parameters = {'key':'AIzaSyCA_H7Ht8GzI2RgYfZJeyPLz8O8c0soCig', 'location': location, 'type':'restaurant' ,'radius': 40000, 'keyword':keyword}
        resp2 = requests.get(G2_baseurl, parameters).text
        results = json.loads(resp2)
        results = results["results"]
        restaurant_by_city = {}
        restaurant_dict_list =  []
        restaurant_dict = {}
        for i in results:
            lat = round(i['geometry']['location']['lat'],0)
            long = round(i['geometry']['location']['lng'],0)
            lat_long = str(lat) + "/" + str(long)
            restaurant_dict = {'Id': str(lat_long +"/" + i['name']),'city':city, 'state':state,'type':'restaurant','keyword':keyword,'name':i['name'],'rating':i['rating'],'coord':lat_long}

            restaurant_dict_list.append(restaurant_dict)
        restaurant_by_city[key] = restaurant_dict_list
        cache_this(key,restaurant_by_city, GOOGLE_CACHE_FILE)
        Update_table(restaurant_by_city[key], GOOGLE_TBL)

        return restaurant_dict_list



def get_yelp_data(city, state, keyword):
    location = str(city + ", " + state)
    keyword = keyword
    term = str(keyword + ", restaurant")
    key = str(location + ", " +  keyword)
    cache_try = get_cached_data(key, YELP_CACHE_FILE)

    if cache_try == None:
        # print("requesting new data")
        Y1_baseurl = 'https://api.yelp.com/v3/businesses/search'
        bear = str("Bearer " + secrets.YELP_API_Key)
        headers = {'Authorization': bear }
        parameters = {'term': term, 'location': location, 'radius':40000, 'limit':50}
        resp1 = requests.get(Y1_baseurl, headers = headers, params = parameters).text
        json_resp = json.loads(resp1)['businesses']
        restaurant_by_city = {}
        restaurant_dict_list =  []
        restaurant_dict = {}
        for i in json_resp:
            lat = round(i['coordinates']['latitude'], 0)
            long = round(i['coordinates']['longitude'],0)
            lat_long = str(lat) + "/" + str(long)
            restaurant_dict = {'Id': str(lat_long +"/" + i['name']), 'city':city, 'state':state,'type':'restaurant','keyword':keyword,'name':i['name'],'rating':i['rating'], 'coord':lat_long}
            restaurant_dict_list.append(restaurant_dict)
        restaurant_by_city[key] = restaurant_dict_list
        cache_this(key,restaurant_by_city, YELP_CACHE_FILE)
        Update_table(restaurant_by_city[key], YELP_TBL)


def cache_this(key, new_cache_content, file):
    try:
        # print("caching new things")
        fw1 = open(file, "r")
        contents = fw1.read()
        contents_json = json.loads(contents)
        fw1.close()
        contents_json.update(new_cache_content)
        combined = json.dumps(contents_json)
        fw2 = open(file, "w")
        fw2.write(combined)
        fw2.close()

    except:
        # print("exeption at caching new things")
        fw = open(file, "w")
        data = json.dumps(new_cache_content)
        fw.write(data)
        fw.close()


def get_cached_data(key, file):
    try:
        # print("at cache try")
        fw = open(file, "r")
        contents = fw.read()
        contents_json = json.loads(contents)
        fw.close()
        if key in contents_json:
            return contents_json[key]
        else:
            return None
    except:
        # print("exeption at cache try")
        return None


def create_db(DBNAME):
    try:
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
    except Error as e:
        print(e)

    query1 = 'DROP TABLE IF EXISTS "Google"'
    conn.execute(query1)
    conn.commit()

    query2 = '''
        CREATE TABLE Google (
            'Id' TEXT NOT NULL,
            'City' TEXT NOT NULL,
            'State' TEXT NOT NULL,
            'Type' TEXT NOT NULL,
            'Keyword' TEXT NOT NULL,
            'Name' TEXT NOT NULL,
            'Rating' FLOAT NOT NULL,
            'coord' TEXT NOT NULL
            );
    '''
    cur.execute(query2)
    conn.commit()

    query1 = 'DROP TABLE IF EXISTS "Yelp"'
    conn.execute(query1)
    conn.commit()

    query2 = '''
        CREATE TABLE Yelp (
            'Id' TEXT NOT NULL,
            'City' TEXT NOT NULL,
            'State' TEXT NOT NULL,
            'Type' TEXT NOT NULL,
            'Keyword' TEXT NOT NULL,
            'Name' TEXT NOT NULL,
            'Rating' FLOAT NOT NULL,
            'coord' TEXT NOT NULL
            );
    '''
    cur.execute(query2)
    conn.commit()
    conn.close()


def populate_tables():
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    fw = open(GOOGLE_CACHE_FILE,"r")
    contents = fw.read()
    contents_json = json.loads(contents)
    fw.close()

    for x in contents_json:
        for c in contents_json[x]:
            Id = c['Id']
            City = c['city']
            State = c['state']
            Type = c['type']
            Keyword = c['keyword']
            Name = c['name']
            Rating = c['rating']
            coord = c['coord']


            query = '''INSERT INTO Google Values (?,?,?,?,?,?,?,?)
            '''
            params = (Id, City, State, Type, Keyword, Name, Rating, coord)
            conn.execute(query, params)
            conn.commit()



    fw = open(YELP_CACHE_FILE, "r")
    contents = fw.read()
    contents_json = json.loads(contents)
    fw.close()

    for x in contents_json:
        for c in contents_json[x]:
            Id = c['Id']
            City = c['city']
            State = c['state']
            Type = c['type']
            Keyword = c['keyword']
            Name = c['name']
            Rating = c['rating']
            coord = c['coord']


            query = '''INSERT INTO Yelp Values (?,?,?,?,?,?,?,?)
            '''
            params = (Id, City, State, Type, Keyword, Name, Rating, coord)
            conn.execute(query, params)
            conn.commit()

    conn.close()


def Update_table(new_content,TABLE_NAME):

    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    for c in new_content:
        Id = c['Id']
        City = c['city']
        State = c['state']
        Type = c['type']
        Keyword = c['keyword']
        Name = c['name']
        Rating = c['rating']
        coord = c['coord']

        query = '''INSERT INTO {} Values (?,?,?,?,?,?,?,?)
        '''.format(TABLE_NAME)
        params = (Id, City, State, Type, Keyword, Name, Rating, coord)

        conn.execute(query, params)
        conn.commit()
    conn.close()

def data_calls(city, state, type1, type2, type3):
        get_google_data(city, state, type1)
        get_google_data(city, state, type2)
        get_google_data(city, state, type3)
        get_yelp_data(city, state, type1)
        get_yelp_data(city, state, type2)
        get_yelp_data(city, state, type3)

def data_call(city, state, company, type1):
    if company == "Google":
        get_google_data(city, state, type1)
    else:
        get_yelp_data(city, state, type1)



def interactive_stuff():
    print("We are creating our database now!")
    create_db(DBNAME)
    print("Populating database tables with cached data")
    populate_tables()
    cuisine_list = ["Indian", "German", "Mexican", "Chinese", "Italian", "Japanese", "Greek", "French","Thai", "Pizza", "Mediterranean", "Spanish"]
    print('Greetings! This program will allow you to explore 3 different types of restaurant options in a city and state of your choice! (Please type "exit" at any time to leave this app)')
    print("Input 'help' for a description of the program and the data visualization options that are available to you!")
    help = "This program will allow you to explore three types of cuisines in a specific city and state. Here are the available commands and their required inputs:\n"
    help += 'Restaurant types: you have a selection of 12 cuisines availabe and they are as follows: Indian, German, Mexican, Chinese, Italian, Japanese, Greek, French, Thai, Pizza, Mediterranean, Spanish.\n'
    help += 'Commands:\n 1) Combined average - this functionality will provide you with the overall average ratings of restaurants that fall into each of the three categories from both Google and Yelp. Example input: '
    help += 'combined average, Ann Arbor, MI: Indian, Mexican, Thai. all input parameters are required.\n'
    help += '2) Specific average allows you to get the same data as combined average but only from one type of rating company Google or Yelp. Example query: specific average, Ann Arbor, MI, Yelp: Indian, Mexican, Thai. All input parameters are required.\n'
    help += "3) Top data allows you to get the top 'x' number of  restaurants in a specific category along with the ratings of those companies from a specific rating company Google or Yelp. Example query: top data, Indian, Ann Arbor, MI, Google, 5. All input parameters are required.\n"
    help += '4) proportion data allows you to see the proportion of the specified cuisines in a given city/state. Example query: proportion data, Ann Arbor, MI, Yelp: Indian, Mexican, Thai.\n'
    help += 'Please enter "help" at any time to get this info again.'
    states1 = ['al', 'ak', 'as', "az", "ar", "ca", "co", "ct", "de", "dc", "fl"]
    states2 = ["ga", "gu", "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me"]
    states3 = ["md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj"]
    states4 = ["nm", "ny", "nc", "nd", "mp", "oh", "ok", "or", "pa", "pr", "ri"]
    states5 = ["sc", "sd", "tn", "tx", "ut", "vt", "vi", "va", "wa", "wv", "wi", "wy"]
    states = states1 + states2 + states3+ states4 + states5

    user_input = ""

    while user_input != "exit":
        user_input = input("Enter a command!")
        first_check = user_input.split()[0].lower()
        if user_input == "help":
            print(help)
            continue
        elif user_input == "exit":
            print("You inputted exit. Goodbye!")
            exit()
        elif user_input == "" or user_input == " ":
            print("This is not enter anything. Please try again!")
            continue
        elif user_input.isnumeric():
            print('Youe entered: ', user_input)
            print("This is not enter a valid command. Please try again!")
            continue
        elif first_check == "combined" or first_check == "specific" or first_check == 'top' or first_check == "proportion":
            if first_check == "combined":
                city = user_input.split(",")[1].strip()
                state = user_input.split(",")[2][0:3].strip()
                type1 = user_input.split(":")[1].split(",")[0].strip()
                type2 = user_input.split(":")[1].split(",")[1].strip()
                type3 = user_input.split(":")[1].split(",")[2].strip()
                data_calls(city, state, type1, type2, type3)
                first_plot(city, state, type1, type2, type3)

            elif first_check == "specific":
                city = user_input.split(",")[1].strip()
                state = user_input.split(",")[2].strip()
                table = user_input.split(":")[0].split(",")[3].strip()
                type1 = user_input.split(":")[1].split(",")[0].strip()
                type2 = user_input.split(":")[1].split(",")[1].strip()
                type3 = user_input.split(":")[1].split(",")[2].strip()
                data_calls(city, state, type1, type2, type3)
                average_rating_query(city, state, type1, type2, type3, table)

            elif first_check == 'top':
                type1 = user_input.split(",")[1].strip()
                city = user_input.split(",")[2].strip()
                state = user_input.split(",")[3].strip()
                table = user_input.split(",")[4].strip()
                limit = user_input.split(",")[5].strip()

                data_call(city, state, table, type1)
                top_in_cat_query(city, state,type1, table, limit)

            elif first_check == "proportion":
                city = user_input.split(",")[1].strip()
                state = user_input.split(",")[2][0:3].strip()
                table = user_input.split(":")[0].split(",")[3].strip()
                type1 = user_input.split(":")[1].split(",")[0].strip()
                type2 = user_input.split(":")[1].split(",")[1].strip()
                type3 = user_input.split(":")[1].split(",")[2].strip()
                data_calls(city, state, type1, type2, type3)
                plot_quantity_query(city, state, type1, type2, type3, table)

        else:
            print("Command not valid. Please try again.")
            continue


def plot_quantity_query(city, state, type1, type2, type3, table, graphing = ""):
    if graphing == "off":

        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
        query = '''
            SELECT Keyword, count(*) FROM "{}" as T
            WHERE T.City == '{}' AND  T.State == '{}'
            AND T.Keyword in ('{}', '{}', '{}')
            group by Keyword
            order by count(*) DESC
        '''.format(table, city, state, type1, type2, type3)

        output = cur.execute(query).fetchall()
        rest_list = []
        num_list = []
        perc_list = []
        for row in output:
            rest_list.append(row[0])
            num_list.append(row[1])

        total = 0
        for i in num_list:
            total += i
        for i in num_list:
            i = i/total
            i = i*100
            i = str(i) + "%"
            perc_list.append(i)

        labels = rest_list
        values = num_list

        return labels, perc_list

    else:
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
        query = '''
            SELECT Keyword, count(*) FROM "{}" as T
            WHERE T.City == '{}' AND  T.State == '{}'
            AND T.Keyword in ('{}', '{}', '{}')
            group by Keyword
            order by count(*) DESC
        '''.format(table, city, state, type1, type2, type3)

        output = cur.execute(query).fetchall()
        rest_list = []
        num_list = []
        for row in output:
            rest_list.append(row[0])
            num_list.append(row[1])
        total = 0
        perc_list = []
        for i in num_list:
            total += i
        for i in num_list:
            x = i/total
            x = x*100
            x = round(x,2)
            perc_list.append(x)

        labels = rest_list
        values = perc_list

        trace = go.Pie(labels=labels, values=values)

        py.plot([trace], filename='basic_pie_chart')
        return rest_list, perc_list

def average_rating_query(city, state, type1, type2, type3, table, graphing = ""):
    if graphing == "off":
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
        query = '''
            SELECT Keyword, Round(AVG(Rating),1) FROM "{}" as T
            WHERE T.City == '{}' AND  T.State == '{}'
            AND T.Keyword in ('{}', '{}', '{}')
            group by Keyword
            order by Round(AVG(Rating),1) DESC
        '''.format(table, city, state, type1, type2, type3)

        output = cur.execute(query).fetchall()
        rest_list = []
        rat_list = []
        for row in output:
            rest_list.append(row[0])
            rat_list.append(row[1])

        return rest_list, rat_list

    else:

        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
        query = '''
            SELECT Keyword, Round(AVG(Rating),1) FROM "{}" as T
            WHERE T.City == '{}' AND  T.State == '{}'
            AND T.Keyword in ('{}', '{}', '{}')
            group by Keyword
            order by Round(AVG(Rating),1) DESC
        '''.format(table, city, state, type1, type2, type3)

        output = cur.execute(query).fetchall()
        rest_list = []
        rat_list = []
        for row in output:
            rest_list.append(row[0])
            rat_list.append(row[1])


        trace0 = go.Bar(
            x=rest_list,
            y = rat_list,
            marker = dict(
                color = 'rgb(150,200,255)',
                line = dict(
                    color = 'rgb(8,10,100)',
                    width = 1.5,
                )
            ),
            opacity = 0.7
        )

        data = [trace0]
        layout = go.Layout(
            title = str('Average ratings of ' + type1 + ", " + type2 + " and " + type3 + " restaurants in " +  city + ", " + state + " by " + table)
        )
        fig = go.Figure(data = data,layout = layout)
        py.plot(fig, filename = 'basic-bar')

        return rest_list, rat_list

def top_in_cat_query(city, state,type1, table, limit, graphing = ""):
    if graphing == "off":
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
        query = '''
            SELECT Name, Rating FROM "{}" as T
            WHERE T.City == '{}' AND  T.State == '{}'
            AND T.Keyword in ('{}')
            order by Rating DESC
            limit {}
        '''.format(table, city, state, type1, limit)

        output = cur.execute(query).fetchall()
        count = 1
        diction = {}
        list = []
        rest_list = []
        rat_list = []

        for row in output:
            rest_list.append(row[0])
            rat_list.append(row[1])


            diction[count] = row[0]
            list.append(diction)
            count += 1
        return diction

    else:

        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
        query = '''
            SELECT Name, Rating FROM "{}" as T
            WHERE T.City == '{}' AND  T.State == '{}'
            AND T.Keyword in ('{}')
            order by Rating DESC
            limit {}
        '''.format(table, city, state, type1, limit)

        output = cur.execute(query).fetchall()
        count = 1
        diction = {}
        list = []
        rest_list = []
        rat_list = []

        for row in output:
            rest_list.append(row[0])
            rat_list.append(row[1])
            diction[count] = row[0]
            list.append(diction)
            count += 1

        trace0 = go.Bar(
            x=rest_list,
            y = rat_list,
            marker = dict(
                color = 'rgb(150,200,255)',
                line = dict(
                    color = 'rgb(8,10,100)',
                    width = 1.5,
                )
            ),
            opacity = 0.7
        )

        data = [trace0]
        layout = go.Layout(
            title = str('Ratings of top ' + str(len(rest_list)) + " " + str(type1) + " restaurants in " +  city + ", " + state + " by " + table)
        )
        fig = go.Figure(data = data,layout = layout)
        py.plot(fig, filename = 'basic-bar')

        return diction

def first_plot(city, state, type1, type2, type3, graphing = ""):
    if graphing == "off":
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
        query = '''
            SELECT G.Keyword, round(AVG(G.Rating),1) , Round(AVG(Y.Rating),1) FROM Google as G JOIN Yelp as Y
            On G.Id == Y.Id
            WHERE G.city == '{}' AND  G.State == '{}'
            AND G.Keyword in ('{}', '{}', '{}')
            Group By G.keyword
        '''.format(city, state, type1, type2, type3)

        output = cur.execute(query).fetchall()
        count = 1
        diction = {}
        list = []
        rest_list = []
        G_rat_list = []
        Y_rat_list = []

        for row in output:
            rest_list.append(row[0])
            G_rat_list.append(row[1])
            Y_rat_list.append(row[2])
        return rest_list, G_rat_list, Y_rat_list

    else:

        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
        query = '''
            SELECT G.Keyword, round(AVG(G.Rating),1) , Round(AVG(Y.Rating),1) FROM Google as G JOIN Yelp as Y
            On G.Id == Y.Id
            WHERE G.city == '{}' AND  G.State == '{}'
            AND G.Keyword in ('{}', '{}', '{}')
            Group By G.keyword
        '''.format(city, state, type1, type2, type3)

        output = cur.execute(query).fetchall()
        count = 1
        diction = {}
        list = []
        rest_list = []
        G_rat_list = []
        Y_rat_list = []

        for row in output:
            rest_list.append(row[0])
            G_rat_list.append(row[1])
            Y_rat_list.append(row[2])

        trace1 = go.Bar(
            x=rest_list,
            y=G_rat_list,
            name='Google'
        )
        trace2 = go.Bar(
            x=rest_list,
            y=Y_rat_list,
            name='Yelp'
        )

        data = [trace1, trace2]
        layout = go.Layout(
            barmode='group',
            title = str('Average ratings of restaurants in three cuisines by  Google and Yelp in ' +  city + ", " + state)
        )

        fig = go.Figure(data=data, layout=layout)
        py.plot(fig, filename='grouped-bar')
        return rest_list, G_rat_list, Y_rat_list


if __name__ == "__main__":
    interactive_stuff()
