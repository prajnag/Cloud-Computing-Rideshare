
'''
This is the worker code, which is run by both-the master and the slave. 
The flag is set to 1 if the code is running as the master, and 0 for the slave.
The master reads from the write queue, and writes into the sync queue.
The slaves read from the read queue and the sync queue, and write into the response queue.
'''
import os.path
from kazoo.client import KazooClient
from kazoo.client import KazooState
import logging
import requests
import pika
import random
import sqlite3  
import json
import ast
import time
from datetime import datetime
import os
import socket 
import time
global flag
flag=0

#Posting a GET request to obtain the PID of the running container based on the hostname
cont_name=socket.gethostname()
url="http://54.87.192.142:74/api/cont/pid/"+str(cont_name)
req_resp=requests.post(url)
cont_id=str(req_resp.text)

#Function to write into the database, which takes in a dictionary as a parameter
def write_func(table_final):
    con=sqlite3.connect("rideshare.db") 
    cur = con.cursor()   
    table = table_final["table"]
    column = table_final["column"]
    insert = table_final["insert"]
    check=table_final["check"]
    #implementing the clear DB function
    if check=="delete":
        if insert=='':
            if table=="users":
                command="delete from users;"
            else:
                command="delete from rides;"
        else:
            #deleting a particular user 
            if table=="users":
                command="delete from users where name =" +"'" + str(insert) + "'"
            else:
                #deleting a particular ride from the DB, based on ride ID
                command="delete from rides where ride_id ="+"'"+str(insert)+"'" 
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
        #command to insert values into the table  
        command="insert into "+ table+" "+"("+stringcol+")"+" values "+"("+stringinsert+")"
    #executing commands for the database
    cur.execute(command)  
    con.commit()   
    con.close()
    return('', 200)


#Function to read from the database, which is called every time there is some content in the read queue
def view(ch, method, props, body):  
    con = sqlite3.connect("rideshare.db") 
    table = json.loads(body)["table"]  
    columns = json.loads(body)["columns"]
    string=""
    for i in columns:
        string+=", "+i
    string=string[1::]
    whr = json.loads(body)["where"]   
    cur = con.cursor() 
    #querying the database
    command="select "+string+" from "+table+" where "+whr  
    cur.execute(command)  

    final={}
    rows = cur.fetchall() 
    if(table=="rides"):
        for i in range(len(rows)):
            final[i]={
                "rideId":str(rows[i][0]),
                "username":str(rows[i][1]),
                "timestamp":str(rows[i][2]),
                "src":str(rows[i][3]),
                "dest":str(rows[i][4]),
                "created_by":str(rows[i][5])
            }
    elif(table=="users"):
        for i in range(len(rows)):
            final[i]={
                "username":str(rows[i][0]),
                "password":str(rows[i][1])
            }

    #writing into the response queue, which is sent back to the orchestrator
    channel.basic_publish(exchange='', routing_key=props.reply_to, properties=pika.BasicProperties(correlation_id = props.correlation_id),body=json.dumps(final))
    channel.basic_ack(delivery_tag=method.delivery_tag)


#These two function obtains the text file stored in the orchestrator, which contains the
#DB write logs, and then executes all the commands 

def populate_rides():
    con = sqlite3.connect("rideshare.db") 
    cur = con.cursor()
    response = requests.get("http://54.87.192.142:80/api/v1/db/populate_rides")
    temp = response.content.decode("utf-8")
    lst = temp.split("\n")
    lst = lst[:-1]
    for i in range (len(lst)):
        cur.execute(str(lst[i]))  
    con.commit()   
    con.close()
    
def populate_users():
    con = sqlite3.connect("rideshare.db") 
    cur = con.cursor()
    response = requests.get("http://54.87.192.142:80/api/v1/db/populate_users")
    temp = response.content.decode("utf-8")
    lst = temp.split("\n")
    lst = lst[:-1]
    for i in range (len(lst)):
        cur.execute(str(lst[i]))  
    con.commit()   
    con.close()

#Connecting to the rabbitMQ server using appropriate credentials 
credentials = pika.PlainCredentials('rabbitmq', 'rabbitmq')
parameters = pika.ConnectionParameters('54.87.192.142', 5672, '/', credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

#if the worker is a master, it executes the following function
def action_master():
    f=open("heyy.txt", "a")
    f.write("Executing Master Function")
    f.close()
    #declares an exchange of type fanout
    channel.exchange_declare(exchange='sync_q', exchange_type='fanout')
    channel.queue_declare(queue='write_q', durable=True)
    print(' [*] Waiting for messages.')
    def callback(ch, method, properties, body):
        temp=json.loads(body.decode("utf-8"))
        #writes into the db every time there is data in the write queue, and publishes the same in the sync queue 
        write_func(temp)
        channel.basic_publish(exchange='sync_q', routing_key='', body=body.decode("utf-8"))
        ch.basic_ack(delivery_tag=method.delivery_tag)
    #master consumes from the write queue 
    channel.basic_consume(queue='write_q', on_message_callback=callback)
    channel.start_consuming()

#if the worker is a slave, it executes the following function
def action_slave():
    f=open("heyy.txt", "a")
    f.write("Executing Slave Function")
    f.close()
    #on starting up, it reads from the db write logs and executes them in its own database 
    rides = populate_rides()
    users = populate_users()
    channel.queue_declare(queue='read_q', durable=True)
    channel.exchange_declare(exchange='sync_q', exchange_type='fanout')
    #declare a temporary queue that is bound to the sync exchange 
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange='sync_q', queue=queue_name)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='read_q', on_message_callback=view)
    #executing db write function every time it consumes from the sync queue
    def callback_sync(ch, method, properties, body):
        print(' [*] got data in syncQ')
        write_func(json.loads(body.decode("utf-8")))

    channel.basic_consume(
        queue=queue_name, on_message_callback=callback_sync, auto_ack=True)
    channel.start_consuming()


#Connecting to Zookeeper, using port 2181
logging.basicConfig()
zk=KazooClient(hosts='54.87.192.142:2181')
zk.start()
#Obtains a list of all the children and returns the smallest PID in all the nodes 
def smallest():
   child=zk.get_children("/election")
   new=[]
   for i in child:
      st='/election/'+str(i)
      data,st= zk.get(st)
      new.append(data.decode())
   new=sorted(new)
   return new[0]

#Watch function which checks if the current container has the PID equal to the smallest PID
def demo_func(event):
    # Create a node with data
    global flag
    new_master_pid = smallest()
    if cont_id==new_master_pid: 
         flag=1
         f=open("heyy.txt", "a")
         channel.stop_consuming()
         f.write("MASTER AFTER DELETE!!")
         f.close()
         action_master()


zk.ensure_path("/election")
#Creating a node with the given path and with data as byte strings of the container PID
zk.create("/election/worker", cont_id.encode(), ephemeral=True,sequence=True)
time.sleep(5)
#Set a watch on all the children 
zk.get_children("/election", watch=demo_func)
#If this container does not have the smallest PID, then it executes slave function, else executes the master function
if cont_id!=smallest() and cont_id!=1 and cont_id!=0:
   action_slave()
else:
   action_master()









