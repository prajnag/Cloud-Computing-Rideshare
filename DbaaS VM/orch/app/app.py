'''
This python program acts as the Orchestrator. It receives HTTP read and write requests from the
rides and users containers, and then forwards it to the appropriate queues.
'''

import threading
import uuid
import random
import sqlite3  
import requests
import json
import ast
import docker 
from datetime import datetime
from flask import Flask, render_template,\
jsonify,request,abort, Response
import pika
import time
from kazoo.client import KazooClient
from kazoo.client import KazooState
import logging
global req_count
global container_ct
req_count=0
container_ct=1
global spawn
spawn=1


app=Flask(__name__)
#connecting to the rabbitMQ server
credentials = pika.PlainCredentials('rabbitmq', 'rabbitmq')
parameters = pika.ConnectionParameters('54.87.192.142', 5672, '/', credentials)
#Opening two files into which the write db operations are written into
f_user = open("users.txt", "a+")
f_rides = open("rides.txt", "a+")
#Used for running Docker containers from the orchestrator 
client = docker.DockerClient(base_url='unix:///var/run/docker.sock')
#running the Docker image of the slave 
client.containers.run('slave_app:latest',privileged=True, detach=True, network='slave_default', restart_policy={"Name":'on-failure'})
client.containers.run('slave_app:latest', detach=True,privileged=True, network='slave_default',restart_policy={"Name":'on-failure'})

#Connecting to the Zookeeper server
logging.basicConfig()
zk=KazooClient(hosts='54.87.192.142:2181')
zk.start()


#Watch function that gets triggered every time a worker node crashes.
def create_new(event):
    global spawn
    if spawn==1:
      cont_obj =client.containers.run('slave_app:latest',privileged=True, detach=True, network='slave_default', restart_policy={"Name":'on-failure'})
      time.sleep(5)
      pid = str(cont_obj.top()['Processes'][0][1])
      children = zk.get_children("/election") 
      #code to set a watch on the newly created node   
      for i in children:
        stri = '/election/'+str(i)
        data, stat = zk.get(stri)
        if data.decode()==pid:
            zk.get(stri, watch = create_new)
        else:
           pass
    spawn=1


#ensuring that a path exists, if not then it creates a path
zk.ensure_path("/election")
time.sleep(5)

#We get all the children of /election and then set a watch on all of them
children = zk.get_children("/election")
for i in children:
    s = '/election/'+str(i)
    zk.exists(s, watch = create_new)


#This function is built using the threading module and is called every 120 seconds, to check
#the number of HTTP read requests and scale up/down the number of slaves accordingly. 
def scaling():
  threading.Timer(120.0, scaling).start()
  global req_count
  #finding the number of slaves that should be running
  n=int(req_count/20)+1
  global container_ct
  #function to scale up
  if n>container_ct:
    temp=n-container_ct
    #we run as many containers as the difference between the number of reqd containers and number of running containers
    for i in range(temp):  
      client = docker.DockerClient(base_url='unix:///var/run/docker.sock')
      cont_obj=client.containers.run('slave_app:latest',privileged=True, detach=True, network='slave_default', restart_policy={"Name":'on-failure'})
      time.sleep(5)
      pid = str(cont_obj.top()['Processes'][0][1])
      children = zk.get_children("/election")
      #setting a watch on the newly created slave
      for i in children:
        stri = '/election/'+str(i)
        data, stat = zk.get(stri)
        if data.decode()==pid:
            zk.get(stri, watch = create_new)
      container_ct+=1
  #scaling down the number of containers
  elif n<container_ct and container_ct!=1:
    #if the flag spawn is 0, the zookeeper watch function does not spawn another worker
    global spawn
    spawn=0
    temp=container_ct-n
    for i in range(temp):  
        client = docker.DockerClient(base_url='unix:///var/run/docker.sock')
        a = list()
        a = client.containers.list()
        pid_dict = dict()
        for i in a:
            if i.attrs['Config']['Image'] =='slave_app:latest':
                pid_dict[i]=i.attrs['State']['Pid']
        pid_dict = sorted(pid_dict.items(), key=lambda kv: kv[1])
        for i in pid_dict:
            if i[1]==0:
                pid_dict.remove(i)
        slave_tuple = pid_dict[-1]
        slave_container_object = slave_tuple[0]
        slave_container_object.stop()
        container_ct-=1
  req_count=0
scaling()

#API to send the contents of the text file which contains the DB write operations for the rides APIs
@app.route("/api/v1/db/populate_rides",methods=["GET"])
def getfile_rides():
    with open("rides.txt", "r+") as f:
        data=f.read()
    return data

#API to send the contents of the text file which contains the DB write operations for the rides APIs
@app.route("/api/v1/db/populate_users",methods=["GET"])
def getfile_users():
    with open("users.txt", "r+") as f:
        data=f.read()
    return data

#function to increment the global request count
def incr_ct():
    global req_count
    req_count+=1

#API that is called when any other API is called
@app.before_request
def incr_ct_before():
    #if the path of the request matches the read db path, the request count is incremented
    if request.path in ('/api/v1/db/read'):
        incr_ct()

#This class is to implement the Remote Procedure Call in RabbitMQ
class Readreq(object):
    #firstly, we initialise a connection to the rabbitMQ server
    def __init__(self):
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        #We declare a queue, which will be used as the read queue
        result = self.channel.queue_declare(queue='', durable=True)
        #here, the callback queue is the rersponse queue
        self.callback_queue = result.method.queue
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)
    def on_response(self, ch, method, props, body):
        #correlation ID is unique for every request, used to make sure that the response obtained is for the corresponding request sent
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, query):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        #publishing the given request into the read queue 
        self.channel.basic_publish(
            exchange='',
            routing_key='read_q',           
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
      
            ),
            body=json.dumps(query))

     
        while self.response is None:
            self.connection.process_data_events()
       
        return json.loads(self.response)

#read database operation
@app.route("/api/v1/db/read",methods=["POST"])
def send_rabbit_read():
    table=request.get_json()["table"]
    columns=request.get_json()["columns"]
    where=request.get_json()["where"]
    #forming a JSON with all the required fields, and further sent to the read queue
    message = {"table":table,"columns":columns,"where":where}
    print("Sent reading data")
    readreq=Readreq()
    response = readreq.call(message)
    return json.dumps(response)
   


#write database operation
@app.route("/api/v1/db/write", methods=["POST"])
def send_rabbit_write():
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    table=request.get_json()["table"]
    insert=request.get_json()["insert"]
    column=request.get_json()["column"]
    check=request.get_json()["check"]
    message = {"table":table,"insert":insert,"column":column,"check":check}
    #forming a JSON with all the required fields, and further sent to the write queue
    f_user = open("users.txt", "a+")
    f_rides = open("rides.txt", "a+")
    if check=="delete":
            if table=="users":
                query = "delete from users where name =" +"'" + str(insert) + "'"
                command= query + "\n"  
                #writing all the commands into the rides and user text files 
                f_user.write(command)    
            else:
                query = "delete from rides where ride_id ="+"'"+str(insert)+"'" 
                command= query + "\n"  
                f_rides.write(command)

    else:
        if(table == "users"):
            finalstr=''
            stringcol=""
            stringinsert=""
            for i in column:
                stringcol+=", "+str(i)
            stringcol=stringcol[1::] 
            for i in insert:
                stringinsert+=", "+"'"+str(i)+"'"
            stringinsert=stringinsert[1::]   
            query = "insert into users "+"("+stringcol+")"+" values "+"("+stringinsert+")"
            command= query + "\n"  
            f_user.write(command)

        else:
            finalstr=''
            stringcol=""
            stringinsert=""
            for i in column:
                stringcol+=", "+str(i)
            stringcol=stringcol[1::] 
            for i in insert:
                stringinsert+=", "+"'"+str(i)+"'"
            stringinsert=stringinsert[1::]   
            query = "insert into rides "+"("+stringcol+")"+" values "+"("+stringinsert+")"
            command= query + "\n"  
            f_rides.write(command)
       
    f_user.close()
    f_rides.close()
    #publishing the message into the write_q
    channel.queue_declare(queue='write_q', durable=True)
    channel.basic_publish(exchange='', routing_key='write_q', body=json.dumps(message))
    print("Sent Writing Data")
    return('',200)

#API to clear the DB and hence the rides and users tables 
@app.route("/api/v1/db/clear",methods=["POST"])
def clear_db():
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel() 
    #write request to clear the db are sent through the write queue
    channel.queue_declare(queue='write_q', durable=True)
    #we empty both the text files logging the write operations
    f_users = open('rides.txt', 'w')
    f_users.close()
    f_rides = open('users.txt','w')
    f_rides.close()
    message = {"table":"users","insert":"","column":["name", "pwd"],"check":"delete"}
    #publishing the messages into the write_q
    channel.basic_publish(
	    exchange='',
	    routing_key='write_q',
	    body=json.dumps(message))
    message = {"table":"rides","insert":"","column":["user","src","dest", "ride_id", "timestamp", "created_by"],"check":"delete"}
    channel.basic_publish(
	    exchange='',
	    routing_key='write_q',
	    body=json.dumps(message))
    connection.close()
    return "Success",200

#API to crash the master container, which is the container with the lowest PID
@app.route("/api/v1/crash/master",methods=["POST"])
def crash_master():
        client = docker.from_env()
        a = list()
        a = client.containers.list()
        pid_dict=dict()
        for i in a:
           #finding all containers which have a particular image, and then storing the PID
           if i.attrs['Config']['Image']=='slave_app:latest':
                pid_dict[i]=i.attrs['State']['Pid']
        pid_dict = sorted(pid_dict.items(), key=lambda kv: kv[1])
        for i in pid_dict:
            if int(i[1])==0:
               pid_dict.remove(i)
        master_tuple = pid_dict[0]
        master_container_object = master_tuple[0]
        pid=[]
        pid.append(str(master_tuple[1]))
        #killing the container with the lowest PID and returning its PID
        master_container_object.kill()
        return(jsonify(pid),200)

#API to crash the slave container, which is the container with the highest PID
@app.route("/api/v1/crash/slave",methods=["POST"])
def crash_slave():
        client = docker.from_env()
        a = list()
        a = client.containers.list()
        pid_dict = dict()
        for i in a:
            if i.attrs['Config']['Image'] =='slave_app:latest':
                pid_dict[i]=i.attrs['State']['Pid']
        pid_dict = sorted(pid_dict.items(), key=lambda kv: kv[1])
        for i in pid_dict:
            if i[1]==0:
                pid_dict.remove(i)
        #taking the container with the highest PID
        slave_tuple = pid_dict[-1]
        slave_container_object = slave_tuple[0]
        pid=[]
        pid.append(str(slave_tuple[1]))
        #killing the container
        slave_container_object.kill()
        return(jsonify(pid),200)


#API to return a list of all the running workers
@app.route("/api/v1/worker/list",methods=["GET"])
def worker_list():
    client = docker.from_env()
    a = list()
    a = client.containers.list()
    pid_dict = {}
    for i in a:
        if i.attrs['Config']['Image'] =='slave_app:latest':
            pid_dict[i]=i.attrs['State']['Pid']
    #Sorting the list based on the PID and returning a JSON object of the same 
    pid_dict = sorted(pid_dict.values())
    return jsonify(pid_dict)

#Running the Flask Application
if __name__ == '__main__':	
	app.debug=True
	app.run(host='0.0.0.0', use_reloader=False)
