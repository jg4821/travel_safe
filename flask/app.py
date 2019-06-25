from flask import Flask, render_template, url_for
import database

# initialize the app
app = Flask(__name__)
app.config['DEBUG'] = True

database.init_db(app)
db = database.db


@app.route("/")
def hello():
    return render_template("home.html")

@app.route("/about")
def about():
    return "<h1>About Page</h1>"

@app.route("/dashboardtest")
def test():
    sql_query = "SELECT * FROM safety_test LIMIT 10"
    result = db.engine.execute(sql_query)
    # print(result)
    return render_template("dashboardtest.html", data=result)

# if __name__ == '__main__': 
#     app.run(debug=True)