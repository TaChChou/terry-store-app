from flask import Flask, url_for, redirect, render_template, request
from ETL import run_pipeline

app = Flask(__name__)

#create a base route
@app.route('/')
def home():
    return render_template('index.html')
#other content goes here

@app.route('/submit', methods = ['POST'])
def submit_form():
    name = request.form['name']
    email = request.form['email']
    address = request.form['address']
    country = request.form['country']
    item = request.form['item']

    run_pipeline(name, email, address, country, item)
    return f"Form submitted! Name: {name}, Email: {email}, Address: {address}, Country: {country}, Item: {item}"
    #return render_template('index.html')

#ends before 'main'
if __name__ == '__main__':
    app.run(debug=True)
