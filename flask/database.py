from flask_sqlalchemy import SQLAlchemy
import os


def init_db(app):
    global db
    # the values of those depend on your setup
    POSTGRES_URL = get_env_variable("POSTGRES_URL")
    POSTGRES_USER = get_env_variable("POSTGRES_USER")
    POSTGRES_PW = get_env_variable("POSTGRES_PASS")
    POSTGRES_DB = get_env_variable("POSTGRES_DB")

    DB_URL = 'postgresql+psycopg2://{user}:{pw}@{url}:5000/{db}'.format(user=POSTGRES_USER,pw=POSTGRES_PW,url=POSTGRES_URL,db=POSTGRES_DB)

    app.config['SQLALCHEMY_DATABASE_URI'] = DB_URL

    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False # silence the deprecation warning

    db = SQLAlchemy(app)

    # db.create_all()

def get_env_variable(name):
    try:
        return os.environ[name]
    except KeyError:
        message = "Expected environment variable '{}' not set.".format(name)
        raise Exception(message)

