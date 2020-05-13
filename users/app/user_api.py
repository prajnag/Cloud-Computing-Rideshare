
import random
import sqlite3  
import requests
import json
import ast
from datetime import datetime
from flask import Flask, render_template,\
jsonify,request,abort, Response
global req_count
req_count=0


app=Flask(__name__)
@app.route('/api/v1/users/hello/<name>')
def hello_world(name):	
	return "Hello from users, %s !" % name
def incr_ct():
    global req_count
    req_count+=1



@app.before_request
def incr_ct_before():
    if request.path not in ('/', '/api/v1/db/write','/api/v1/db/read','/api/v1/_count','/api/v1/db/clear'):
        incr_ct()

@app.route("/api/v1/_count", methods=["GET"])
def get_ct():
    if request.method != "GET":  
        return('', 405)
    final=[]
    final.append(req_count)
    return(jsonify(final), 200)

@app.route("/api/v1/_count", methods=["DELETE"])
def del_ct():
    if request.method != "DELETE":  
        return('', 405)
    global req_count
    req_count=0
    return(jsonify({}),200)
import string
#Function to check if the user password is of hexadecimal type or not 
def is_hex(s):
     hex_digits = set(string.hexdigits)
     return all(c in hex_digits for c in s)

#Adding a new user to the database 
@app.route('/api/v1/users', methods=["PUT"]) #ENCODING- 201, 400, 405
def add_new_user():
    if request.method != "PUT":  
        return('', 405)
    username = request.get_json()["username"]  
    password = request.get_json()["password"]
    password=password.upper()
    if(len(str(password))!=40 or is_hex(str(password))==False):
        return('Invalid Password', 400)
    user = {}
    password=password.lower()
    user["table"] = "users"
    user["columns"] = ["name","pwd"]
    user["where"] = "name='"+username+"'"
    r = json.dumps(user)
    loaded_r = json.loads(r)
    headers = {'content-type': 'application/json'}
    #Reading from the user db to check if the user already exists 
    req_resp=requests.post("http://54.87.192.142:80/api/v1/db/read",data = r, headers = headers)
    temp=str(req_resp.text)
    temp=json.loads(temp)
    if(bool(temp)==True): #EMPTY DICTIONARY
        return('User already exists', 400)
    else:
        add_user = {}
        add_user_j = {}
        add_user["insert"] = [str(username),str(password)]
        add_user["column"] = ["name","pwd"]
        add_user["table"] = "users"
        add_user["check"]="write"
        r = json.dumps(add_user)
        loaded_r = json.loads(r)
        headers = {'content-type': 'application/json'}
        req_resp=requests.post("http://54.87.192.142:80/api/v1/db/write",data = r, headers = headers)
        return('', 201) #successful create


#Removing a particular user based on username 
@app.route("/api/v1/users/<name>",methods = ["DELETE"])  
def removeuser(name):
    if request.method != "DELETE":  
        return('', 405)
    user = {}
    user["table"] = "users"
    user["columns"] = ["name","pwd"]
    user["where"] = "name='"+name+"'"
    r = json.dumps(user)
    loaded_r = json.loads(r)
    headers = {'content-type': 'application/json'}
    req_resp=requests.post("http://54.87.192.142:80/api/v1/db/read",data = r, headers = headers)
    temp=str(req_resp.text)
    temp=json.loads(temp)
    if(bool(temp)==False): #EMPTY DICTIONARY
        return('Username Not Found', 400)
    else:
        add_user = {}
        add_user_j = {}
        add_user["insert"] = str(name)
        add_user["column"] = ["name","pwd"]
        add_user["table"] = "users"
        add_user["check"]="delete"
        r = json.dumps(add_user)
        loaded_r = json.loads(r)
        headers = {'content-type': 'application/json'}
        req_resp=requests.post("http://54.87.192.142:80/api/v1/db/write",data = r, headers = headers)
        return('',200)


#Get a list of all the users in the database 
@app.route('/api/v1/users')
def list_all_users():
    if request.method != "GET":  
        return('', 405)
    user = {}
    user["table"] = "users"
    user["columns"] = ["name","pwd"]
    user["where"] = "exists(select 1)"
    r = json.dumps(user)
    loaded_r = json.loads(r)
    headers = {'content-type': 'application/json'}
    req_resp=requests.post("http://54.87.192.142:80/api/v1/db/read",data = r, headers = headers)
    temp=str(req_resp.text)
    temp=json.loads(temp)
    #If there are no users in the database currently 
    if(bool(temp)==False):
        return("No users", 204)
    new=[]
    for i in temp:
        new.append(temp[i]["username"])
    return(jsonify(new), 200)

#Running the flask app 
if __name__ == '__main__':	
	app.debug=True
	app.run(host='0.0.0.0',debug=True)
