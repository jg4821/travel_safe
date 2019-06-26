from flask import Flask, render_template, url_for, request, jsonify
import database
from forms import SearchForm
import os


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
safety_table = Table('safety_test', metadata, autoload=True,
                           autoload_with=engine)
Session = sessionmaker(bind=engine)
session = Session()



@app.route("/")
def hello():
    return render_template("home.html")

@app.route("/about")
def about():
    return render_template("about.html")

@app.route("/dashboardtest")
def dashboardtest():
    sql_query = "SELECT * FROM safety_test LIMIT 10"
    result = db.engine.execute(sql_query)
    # print(result)
    return render_template("dashboardtest.html", data=result)

@app.route("/dropdowntest")
def dropdowntest():
    return render_template("dropdowntest.html")


@app.route('/droptest', methods=['GET', 'POST'])
def droptest():
    form = SearchForm()
    form.state.choices = [(row.state, row.state) for row in session.query(safety_table).filter_by(country='China')\
                            .distinct(safety_table.c.state).order_by(safety_table.c.state).all()]
    form.city.choices = [(row.city, row.city) for row in session.query(safety_table).filter_by(country='China',state='Sichuan')\
                            .distinct(safety_table.c.city).order_by(safety_table.c.city).all()]

    if request.method == 'POST':
        # row = session.query(safety_table).filter_by(state=form.state.data).first()
        return '<h1>Country: {}, State: {}, City: {}</h1>'.format(form.country.data, form.state.data, form.city.data)

    return render_template('droptest.html', form=form)

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

# if __name__ == '__main__': 
#     app.run(debug=True)

