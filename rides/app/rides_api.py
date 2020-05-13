'''
This Flask application contains all the ride APIs, which send DB read and write reqs 
to the Orchestrator
'''
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

#increment global request count
def incr_ct():
    global req_count
    req_count+=1

#function that is called with every API being called, increments the req_count variable by 1
@app.before_request
def incr_ct_before():
    if request.path not in ('/api/v1/db/clear','/', '/api/v1/db/write','/api/v1/db/read','/api/v1/_count'):
        incr_ct()
 
#API to return the count of the number of reqs made
@app.route("/api/v1/_count", methods=["GET"])
def get_ct():
    if request.method != "GET":  
        return('', 405)
    final=[]
    final.append(req_count)
    return(jsonify(final), 200)


#Reset the count to 0
@app.route("/api/v1/_count", methods=["DELETE"])
def del_ct():
    if request.method != "DELETE":  
        return('', 405)
    global req_count
    req_count=0
    return(jsonify({}),200)


#Create a new ride 
@app.route("/api/v1/rides", methods=["POST"])
def create_new_ride():
    if request.method != "POST":  
        return('', 405)
    user = request.get_json()["created_by"]  
    time_stamp = request.get_json()["timestamp"]
    time_stamp_str = str(time_stamp) 
    src = request.get_json()["source"]  
    dest = request.get_json()["destination"]
    temp=True
    while(temp):
        check=str(random.randint(1,100000))
        ride_id=check
        upcoming_rides = {}
        upcoming_rides["table"] = "rides"
        upcoming_rides["columns"] = ["ride_id","user","timestamp","src","dest", "created_by"]
        upcoming_rides["where"] = "ride_id='"+check+"'"
        r = json.dumps(upcoming_rides)
        loaded_r = json.loads(r)
        headers = {'content-type': 'application/json'}
        req_resp=requests.post("http://54.87.192.142:80/api/v1/db/read",data = r, headers = headers)
        temp=str(req_resp.text)
        temp=json.loads(temp)
        if(bool(temp)==True):
            print(check,"exists")
    check={}
    #to check if a valid source or destination 
    if(int(src)<1 or int(dest)<1 or int(src)>198 or int(dest)>198):
        return('Invalid Source/Destination', 400)
    check["table"] = "users"
    check["columns"] = ["name","pwd"]
    check["where"] = "name='"+user+"'"
    r = json.dumps(check)
    loaded_r = json.loads(r)
    #to check if user creating the ride exists, which is sent across the load balancer 
    headers = {'content-type': 'application/json','Origin':'34.206.113.190'} 
    req_resp=requests.get("http://load-balancer-1669223086.us-east-1.elb.amazonaws.com/api/v1/users", headers = headers)
    if user  not in req_resp.text:
       return ('User does not exist', 400)
    else:
        #If the user exists, send a HTTP write request to Orchestartor
        add_user={}
        add_user["insert"]=[str(user), str(src), str(dest), ride_id, str(time_stamp), str(user)]
        add_user["column"]=["user", "src", "dest", "ride_id", "timestamp", "created_by"]
        add_user["table"]="rides"
        add_user["check"]="write"
        r = json.dumps(add_user)
        loaded_r = json.loads(r)
        headers = {'content-type': 'application/json'}
        req_resp=requests.post("http://54.87.192.142:80/api/v1/db/write",data = r, headers = headers)
        return('', 201)

#Listing all the rides between a given source and destination
@app.route('/api/v1/rides')
def list_upcoming_rides():
    source = request.args.get('source', default="1",type=str)
    print(type(source))
    destination = request.args.get('destination', default="1", type=str)
    if request.method != "GET":  
        return('', 405)
    if(int(source)<1 or int(destination)<1 or int(source)>198 or int(destination)>198):
        return('Invalid Source/Destination', 400)
    timestamp_now = datetime.now()
    datetimeobject = datetime.strptime(str(timestamp_now)[0:18],'%Y-%m-%d %H:%M:%S')
    timestamp_new = datetimeobject.strftime('%d-%m-%Y:%S-%M-%H')
    timestamp_final = datetime.strptime(timestamp_new,'%d-%m-%Y:%S-%M-%H')
    upcoming_rides = {}
    upcoming_rides["table"] = "rides"
    upcoming_rides["columns"] = ["ride_id","user","timestamp","src","dest", "created_by"]
    upcoming_rides["where"] = "src ="+ "'"+ source +"'"+" and "+"dest="+ "'"+ destination + "'" 
    r = json.dumps(upcoming_rides)
    loaded_r = json.loads(r)
    headers = {'content-type': 'application/json'}
    req_resp=requests.post("http://54.87.192.142:80/api/v1/db/read",data = r, headers = headers)
    temp=str(req_resp.text)
    temp=json.loads(temp)
    final = []
    border=[]
    for i in temp:
        notfinal={}
        temp_date = temp[i]["timestamp"][0:19]
        temp_date_final = datetime.strptime(temp_date,'%d-%m-%Y:%S-%M-%H')
        if temp_date_final>timestamp_final:
            if(temp[i]["rideId"] not in border):
                notfinal["rideId"]=temp[i]["rideId"]
                notfinal["username"]=temp[i]["created_by"]
                notfinal["timestamp"]=temp[i]["timestamp"]
                final.append(notfinal)
                border.append(temp[i]["rideId"])
    if(bool(temp)==False): #EMPTY DICTIONARY
        return('No Upcoming Rides', 204)
    return(jsonify(final),200)
#LIST NUMBER OF RIDES
@app.route("/api/v1/rides/count", methods=["GET"])
def count_rides():
    if request.method != "GET":  
        return('', 405)
    user = {}
    user["table"] = "rides"
    user["columns"] = ["ride_id","user","timestamp","src","dest", "created_by"]
    user["where"] = "exists(select 1)"
    r = json.dumps(user)
    loaded_r = json.loads(r)
    headers = {'content-type': 'application/json'}
    req_resp=requests.post("http://54.87.192.142:80/api/v1/db/read",data = r, headers = headers)
    temp=str(req_resp.text)
    temp=json.loads(temp)
    if(bool(temp)==False):
        return("No rides", 204)
    new=[]
    for i in temp:
        new.append(int(str(temp[i]["rideId"])))
    new=set(new)
    final=[len(new)]
    
    return(jsonify(final),200)

#List all the details of a ride based on its ride id   
@app.route("/api/v1/rides/<rideid>")
def display_ride_details(rideid):
    if request.method != "GET":  
        return('', 405)
    upcoming_rides = {}
    upcoming_rides["table"] = "rides"
    upcoming_rides["columns"] = ["ride_id","user","timestamp","src","dest", "created_by"]
    upcoming_rides["where"] = "ride_id='"+rideid+"'"
    r = json.dumps(upcoming_rides)
    loaded_r = json.loads(r)
    headers = {'content-type': 'application/json'}
    req_resp=requests.post("http://54.87.192.142:80/api/v1/db/read",data = r, headers = headers)
    temp=str(req_resp.text)
    temp=json.loads(temp)
    if(bool(temp)==False):
        return Response(status = 204)
    final={}
    final["rideId"]=temp["0"]["rideId"] #all timestamps should be same, also send source and dest in the response object 
    final["Created_by"]=temp["0"]["created_by"]
    final["Timestamp"]=temp["0"]["timestamp"]
    final["source"]=temp["0"]["src"]
    final["destination"]=temp["0"]["dest"]
    templ=[]
    for i in temp:
        templ.append(str(temp[i][u'username']))
    final["users"]=templ
    return(jsonify(final), 200)
    
"""
 {
"username" : "{username}"
}
"""

#Join an existing ride provided the user creating the ride exists 
@app.route("/api/v1/rides/<rideid>", methods=["POST"])
def join_existing_ride(rideid):
    if request.method != "POST":  
        return('', 405)
    user = request.get_json()["username"] 
    rides = {}
    rides["table"] = "rides"
    rides["columns"] = ["ride_id","user","timestamp","src","dest", "created_by"]
    rides["where"] = "ride_id='"+rideid+"'"
    r = json.dumps(rides)
    loaded_r = json.loads(r)
    headers = {'content-type': 'application/json'}
    req_resp=requests.post("http://54.87.192.142:80/api/v1/db/read",data = r, headers = headers)
    temp=str(req_resp.text)
    temp=json.loads(temp)
    if(bool(temp)==False): #NO RIDE EXISTS
        return('', 204) 
    check={}
    check["table"] = "users"
    check["columns"] = ["name","pwd"]
    check["where"] = "name='"+user+"'"
    r = json.dumps(check)
    loaded_r = json.loads(r) #Checks if the user exists by sending a req across load balancer
    headers = {'content-type': 'application/json', 'Origin':'34.204.120.31'}
    req_resp=requests.get("http://load-balancer-1669223086.us-east-1.elb.amazonaws.com/api/v1/users", headers = headers)
    if(str(user) not in req_resp.text): #EMPTY DICTIONARY
        return('User does not exist', 400)
    else:    
        ride_id=temp["0"]["rideId"] 
        time_stamp=temp["0"]["timestamp"]
        src=temp["0"]["src"]
        dest=temp["0"]["dest"]
        created_by=temp["0"]["username"]
        add_user={}
        add_user["insert"]=[str(user), str(src), str(dest), ride_id, str(time_stamp), created_by]
        add_user["column"]=["user", "src", "dest", "ride_id", "timestamp", "created_by"]
        add_user["table"]="rides"
        add_user["check"]="write"
        r = json.dumps(add_user)
        loaded_r = json.loads(r)
        headers = {'content-type': 'application/json'}
        requests.post("http://54.87.192.142:80/api/v1/db/write",data = r, headers = headers)
        return('', 200)


#Delete a ride given its ride ID
@app.route("/api/v1/rides/<rideid>",methods = ["DELETE"])  
def deleteride(rideid):
    if request.method != "DELETE":  
        return('', 405)
    rides = {}
    rides["table"] = "rides"
    rides["columns"] = ["ride_id","user","timestamp","src","dest", "created_by"]
    rides["where"] = "ride_id='"+rideid+"'"
    r = json.dumps(rides)
    loaded_r = json.loads(r)
    headers = {'content-type': 'application/json'}
    req_resp=requests.post("http://54.87.192.142:80/api/v1/db/read",data = r, headers = headers)
    temp=str(req_resp.text)
    temp=json.loads(temp)
    if(bool(temp)==False): #EMPTY DICTIONARY
        return('', 400)
    else:
        add_user={}
        add_user["insert"]=str(rideid)
        add_user["column"]=["user", "src", "dest", "ride_id", "timestamp", "created_by"]
        add_user["table"]="rides"
        add_user["check"]="delete"
        r = json.dumps(add_user)
        loaded_r = json.loads(r)
        headers = {'content-type': 'application/json'}
        requests.post("http://54.87.192.142:80/api/v1/db/write",data = r, headers = headers)
        return('', 200)


#Running the Flask App
if __name__ == '__main__':	
	app.debug=True
	app.run(host='0.0.0.0', debug=True)
