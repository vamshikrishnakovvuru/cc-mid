from flask import Flask, request, render_template, redirect, session, url_for
import os
import sqlite3
import re
import mysql.connector
import pandas as pd
from mysql.connector import errorcode
from flask_mysqldb import MySQL
from flask_session import Session


currentlocation = os.path.dirname(os.path.abspath(__file__))

myapp = Flask(__name__)
myapp.secret_key = '@dkjgfjgfhkj jxbjljv kjxgvljklkj'

UPLOAD_FOLDER = 'static/files'
myapp.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

config = {
  'host':'cc-skywalkers.mysql.database.azure.com',
  'user':'skywalkers@cc-skywalkers',
  'password':'cloudcloud@1234',
  'database':'ccmiddb',
  'ssl_ca': 'ssl\BaltimoreCyberTrustRoot.crt.pem',
}


@myapp.route('/',methods=['GET','POST'])
def homepage():
    msg=''
    if request.method == 'POST' and 'username' in request.form and 'password' in request.form:
        username = request.form['username'] 
        password = request.form['password']
        conn = mysql.connector.connect(**config)
        cur = conn.cursor()
        cur.execute('SELECT * FROM users WHERE username = %s AND password = %s', (username, password, ))
        user = cur.fetchone()
        if user: 
            session['loggedin'] = True
            session['username'] = username
            return redirect(url_for('profile'))
        else:
            # Account doesnt exist
            msg = 'Incorrect username/password!'
    return render_template("homepage.html",msg=msg)
    

@myapp.route('/logout')
def logout():
   session.pop('username', None)
   return render_template("homepage.html")


@myapp.route('/register', methods=['GET', 'POST'])
def register():
    msg = ''
    if request.method == 'POST' and 'username' in request.form and 'password' in request.form:
        username = request.form['username']
        password = request.form['password']
        email = request.form['email']
        conn = mysql.connector.connect(**config)
        cur = conn.cursor()
        cur.execute('SELECT * FROM users WHERE username = %s', (username,))
        user = cur.fetchone()
        if user:
            msg = 'Account already exists!'
        elif not re.match(r'[^@]+@[^@]+\.[^@]+', email):
            msg = 'Invalid email address!'
        elif not re.match(r'[A-Za-z0-9]+', username):
            msg = 'Username must contain only characters and numbers!'
        elif not username or not password or not email:
            msg = 'Please fill out the form!'
        else:
            cur.execute('INSERT INTO users VALUES (%s, %s,%s)', (username,email, password,))
            conn.commit()
            session['loggedin'] = True
            session['username'] = username
            return redirect('profile')
    return render_template("register.html",msg=msg)
            

@myapp.route('/profile',methods=['GET','POST'])
def profile():
    if 'loggedin' in session:
        conn = mysql.connector.connect(**config)
        cur = conn.cursor()
        cur.execute('SELECT * FROM users WHERE username = %s', (session['username'],))
        user = cur.fetchone()
        return render_template('profile.html',user=user) 
    return redirect(url_for(homepage))

@myapp.route('/Search', methods=['GET','POST'])
def Search():
    msg = ''
    if request.method == 'POST' and 'search' in request.form :
        number = request.form['search']
        if not re.match(r'\d+', number):
             msg = "enter a valid household number"
        else:
            conn = mysql.connector.connect(**config)
            cur = conn.cursor()
        # testquery="SELECT h.HSHD_NUM, t.BASKET_NUM, t.PURCHASE_, p.DEPARTMENT, p.COMMODITY,t.SPEND, t.UNITS, t.STORE_R, t.WEEK_NUM, t.YEAR, h.L, h.AGE_RANGE,h.MARITAL,h.INCOME_RANGE, h.HOMEOWNER, h.HSHD_COMPOSITION, h.HH_SIZE, h.CHILDREN FROM households AS h RIGHT JOIN transactions AS t ON h.HSHD_NUM = t.HSHD_NUM RIGHT JOIN products AS p ON t.PRODUCT_NUM = p.PRODUCT_NUM where h.HSHD_NUM=%d"
            #args=[10]
            #cur.callproc('SingleHshdPull', args)
            cur.execute("SELECT h.HSHD_NUM, t.BASKET_NUM, t.PURCHASE_, p.DEPARTMENT, p.COMMODITY,t.SPEND, t.UNITS, t.STORE_R, t.WEEK_NUM, t.YEAR, h.L, h.AGE_RANGE,h.MARITAL,h.INCOME_RANGE, h.HOMEOWNER, h.HSHD_COMPOSITION, h.HH_SIZE, h.CHILDREN FROM households AS h RIGHT JOIN transactions AS t ON h.HSHD_NUM = t.HSHD_NUM RIGHT JOIN products AS p ON t.PRODUCT_NUM = p.PRODUCT_NUM where h.HSHD_NUM=%s",(number,))
            data=cur.fetchall()
            if data:
                return render_template('Search.html', data= data)
            else:
                msg="Not Data Found for the input "
                return render_template('Search.html', msg=msg)
        return render_template('Search.html', msg=msg)
    else:
        conn = mysql.connector.connect(**config)
        cur = conn.cursor()
        testquery="SELECT h.HSHD_NUM, t.BASKET_NUM, t.PURCHASE_, p.DEPARTMENT, p.COMMODITY,t.SPEND, t.UNITS, t.STORE_R, t.WEEK_NUM, t.YEAR, h.L, h.AGE_RANGE,h.MARITAL,h.INCOME_RANGE, h.HOMEOWNER, h.HSHD_COMPOSITION, h.HH_SIZE, h.CHILDREN FROM households AS h RIGHT JOIN transactions AS t ON h.HSHD_NUM = t.HSHD_NUM RIGHT JOIN products AS p ON t.PRODUCT_NUM = p.PRODUCT_NUM where h.HSHD_NUM=10"
        #args=[10]
        #cur.callproc('SingleHshdPull', args)
        cur.execute(testquery)
        data=cur.fetchall()
        return render_template('Search.html', data= data)
    

@myapp.route('/dashboard')
def dashboard():
   return render_template("dashboard.html")

@myapp.route('/upload', methods=['GET','POST'])
def upload():
    msg = ''
    if request.method == 'POST':
        hdata=request.files['households']
        tdata=request.files['transactions']
        pdata=request.files['products']
        conn = mysql.connector.connect(**config)
        cur = conn.cursor()
        if hdata.filename == '' or tdata.filename == '' or pdata.filename == '' :
            msg='No Files passed'
            return render_template('upload.html', msg=msg)
        else:
            file_path = os.path.join(myapp.config['UPLOAD_FOLDER'],hdata.filename)
            hdata.save(file_path)
            col_names=['HSHD_NUM','L','AGE_RANGE','MARITAL','INCOME_RANGE','HOMEOWNER','HSHD_COMPOSITION','HH_SIZE','CHILDREN']
            csvData = pd.read_csv(file_path,names=col_names,header=0)
            for i,row in csvData.iterrows():
                query='INSERT INTO households (HSHD_NUM,L,AGE_RANGE,MARITAL,INCOME_RANGE,HOMEOWNER,HSHD_COMPOSITION,HH_SIZE,CHILDREN) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                value = (row['HSHD_NUM'],row['L'],row['AGE_RANGE'],row['MARITAL'],row['INCOME_RANGE'],row['HOMEOWNER'],row['HSHD_COMPOSITION'],row['HH_SIZE'],row['CHILDREN'])
                print(value)
                cur.execute(query,value)
                conn.commit()
            #transaction data
            file_path = os.path.join(myapp.config['UPLOAD_FOLDER'],tdata.filename)
            tdata.save(file_path)
            col_names=['TRANS_ID','BASKET_NUM','HSHD_NUM','PURCHASE_','PRODUCT_NUM','SPEND','UNITS','STORE_R','WEEK_NUM','YEAR']
            csvData = pd.read_csv(file_path,names=col_names,header=0)
            for i,row in csvData.iterrows():
                query='INSERT INTO transactions (TRANS_ID,BASKET_NUM,HSHD_NUM,PURCHASE_,PRODUCT_NUM,SPEND,UNITS,STORE_R,WEEK_NUM,YEAR) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                value = (row['TRANS_ID'],row['BASKET_NUM'],row['HSHD_NUM'],row['PURCHASE_'],row['PRODUCT_NUM'],row['SPEND'],row['UNITS'],row['STORE_R'],row['WEEK_NUM'],row['YEAR'])
                cur.execute(query,value)
                conn.commit()
            #Products data
            file_path = os.path.join(myapp.config['UPLOAD_FOLDER'],pdata.filename)
            pdata.save(file_path)
            col_names=['PRODUCT_NUM','DEPARTMENT','COMMODITY','BRAND_TY','NATURAL_ORGANIC_FLAG']
            csvData = pd.read_csv(file_path,names=col_names,header=0)
            for i,row in csvData.iterrows():
                query='INSERT INTO products (PRODUCT_NUM,DEPARTMENT,COMMODITY,BRAND_TY,NATURAL_ORGANIC_FLAG) VALUES (%s,%s,%s,%s,%s)'
                value = (row['PRODUCT_NUM'],row['DEPARTMENT'],row['COMMODITY'],row['BRAND_TY'],row['NATURAL_ORGANIC_FLAG'])
                cur.execute(query,value)
                conn.commit()
            msg='Sucessfully Inserted data !!!!!'
            return render_template("upload.html", msg=msg)
    else:
        msg="unable to insert data"
        return render_template("upload.html")
    
    
if __name__=="__main__":
    myapp.run(debug=True)