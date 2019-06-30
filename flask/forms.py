from flask_wtf import FlaskForm 
from wtforms import SelectField
from werkzeug.datastructures import MultiDict
import pickle
import os


cwd = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(cwd,'country.pkl'), 'rb') as inputfile:
    country_file = pickle.load(inputfile)

''' Input Form for user to submit selected global city '''
class SearchForm(FlaskForm):
    country = SelectField('country', choices=country_file)
    state = SelectField('state', choices=[]) 
    city = SelectField('city', choices=[])

