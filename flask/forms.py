from flask_wtf import FlaskForm 
from wtforms import SelectField
import pickle
import os


cwd = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(cwd,'country.pkl'), 'rb') as inputfile:
    country = pickle.load(inputfile)


class SearchForm(FlaskForm):
    country = SelectField('country', choices=country)
    state = SelectField('state', choices=[]) 
    city = SelectField('city', choices=[])

