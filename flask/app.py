from flask import Flask, render_template, url_for, request, jsonify
import database
from forms import SearchForm
import os

import plotly
import plotly.graph_objs as go
import pandas as pd
import numpy as np
import datetime
import json
import copy
import sys
import pdb


# initialize the app
app = Flask(__name__)
app.config['DEBUG'] = True
SECRET_KEY = os.urandom(32)
app.config['SECRET_KEY'] = SECRET_KEY

# initialize database and load existing table
database.init_db(app)
db = database.db

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker

engine = db.get_engine()
metadata = MetaData()
safety_table = Table('safety_score', metadata, autoload=True,
                           autoload_with=engine)
Session = sessionmaker(bind=engine)
session = Session()



@app.route('/', methods=['GET', 'POST'])
def index():
    form = SearchForm()

    if request.method == 'POST':
        max_date_query = '''select max("mDate") from safety_score'''
        max_date = engine.execute(max_date_query).fetchall()
        start_date_str,end_date_str = parse_date(max_date)

        sql_query = '''select "mDate", avg("SafetyScore"*Y.scaledMention) \
                    from (select *, 1.00*("numOfMentions"+1-X.minMention)/X.range as scaledMention \
                    from (select *, min("numOfMentions") over (partition by "mDate") as minMention, \
                    max("numOfMentions") over (partition by "mDate")+1-min("numOfMentions") over (partition by "mDate") as range \
                    from safety_score where "country"='{}' and "state"='{}' \
                    and "city"='{}' and "mDate" between date('{}') and date('{}')) X) \
                    Y group by "mDate" order by "mDate" ASC'''.format(form.country.data,form.state.data,form.city.data,start_date_str,end_date_str)
        result = engine.execute(sql_query).fetchall()
        title = ''+form.city.data+', '+form.state.data+', '+form.country.data
        bar, remove_date_list, start_d, end_d = create_plot(result, title, max_date[0][0])

        top_events_query = '''select top_events.*, events."SOURCEURL" \
                        from (select rank_filter.* from (select *, \
                        rank() over (partition by "mDate" order by "SafetyScore"*Y.scaledMention ASC) as rnk \
                        from (select "GLOBALEVENTID","mDate","SafetyScore", 1.00*("numOfMentions"+1-X.minMention)/X.range as scaledMention \
                        from (select *, min("numOfMentions") over (partition by "mDate") as minMention, \
                        max("numOfMentions") over (partition by "mDate") +1-min("numOfMentions") over (partition by "mDate") as range \
                        from safety_score where "country"='{}' and "state"='{}' and "city"='{}' and "mDate" between \
                        date('{}') and date('{}')) X) Y) rank_filter where rnk <= 3) top_events inner join events \
                        on top_events."GLOBALEVENTID"=events."GLOBALEVENTID"'''.format(form.country.data,form.state.data,form.city.data,start_date_str,end_date_str)
        top_events = engine.execute(top_events_query).fetchall()
        parsed_events = parse_events(top_events, max_date[0][0])
        return render_template('graph.html', form=form, plot=bar, remove_dates=remove_date_list, data=parsed_events, start_date=start_d, end_date=end_d)

    return render_template('index.html', form=form)

@app.route('/state/<country>')
def state(country):
    states = session.query(safety_table).filter_by(country=country) \
                    .distinct(safety_table.c.state).order_by(safety_table.c.state).all()
    stateArray = []

    for sta in states:
        stateObj = {}
        stateObj['id'] = sta.state
        stateObj['name'] = sta.state
        stateArray.append(stateObj)
    return jsonify({'states' : stateArray})

@app.route('/city/<state>/<country>')
def city(country,state):
    cities = session.query(safety_table).filter_by(country=country,state=state) \
                    .distinct(safety_table.c.city).order_by(safety_table.c.city).all()
    cityArray = []

    for city in cities:
        cityObj = {}
        cityObj['id'] = city.city
        cityObj['name'] = city.city
        cityArray.append(cityObj)
    return jsonify({'cities' : cityArray})



def parse_date(date):
    start_date_str = (date[0][0]-datetime.timedelta(days=89)).strftime("%Y-%m-%d")
    end_date_str = date[0][0].strftime("%Y-%m-%d")
    return start_date_str, end_date_str

def parse_events(events, end_date):
    parsed = []
    numdays = 90
    base = datetime.date.today()
    diff = base - end_date
    for e in events:
        row = ''+str(e[0])+','+(e[1]+diff).strftime("%Y-%m-%d")+','+str(e[2]*float(e[3]))+','+str(e[4])+','+str(e[5])
        parsed.append(row)
    return parsed

def create_plot(result, title, end_date):
    x = [tu[0] for tu in result]
    y = [tu[1] for tu in result]

    numdays = 90
    base = datetime.date.today()
    date_list = [base - datetime.timedelta(days=x) for x in range(0, numdays)][::-1]
    remove_list = copy.deepcopy(date_list)

    start_d = [date_list[0].strftime('%Y-%m-%d')]
    end_d = [date_list[-1].strftime('%Y-%m-%d')]

    y_modified = [0]*numdays
    diff = base - end_date
    for ind in range(len(x)):
        elem = x[ind]
        remove_list.remove(elem+diff)
        insert_ind = date_list.index(elem+diff)
        y_modified[insert_ind] = y[ind]

    remove_list = [i.strftime('%Y-%m-%d') for i in remove_list]

    df = pd.DataFrame({'x': date_list, 'y': y_modified})

    graph = dict(
                data = [
                    go.Bar(
                        x=df['x'], 
                        y=df['y']
                    )
                ],
                layout = go.Layout(title = go.layout.Title(text = title), 
                                    xaxis = go.layout.XAxis(title = go.layout.xaxis.Title(text = 'Date')), 
                                    yaxis = go.layout.YAxis(title = go.layout.yaxis.Title(text = 'Safety Level')))
            )

    graphJSON = json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)
    return graphJSON, remove_list, start_d, end_d



if __name__ == '__main__': 
    app.run(port="80", host="0.0.0.0",debug=True)

