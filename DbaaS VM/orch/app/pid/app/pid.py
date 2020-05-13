'''
This container is used for a hostname-PID mapping. 
Every time a new slave is spawed, it posts a GET request with its own hostname to this 
particular container, running on port 74. 
This API then replies with the corresponding PID of that particular hostname. This runs a Flask app
'''

from flask import Flask, render_template,\
jsonify,request,abort, Response
import docker
app=Flask(__name__)


@app.route("/api/cont/pid/<hostname>", methods=["POST"])
def work(hostname):
   client = docker.DockerClient(base_url='unix:///var/run/docker.sock')
   #obtaining the container object on the basis of its PID
   cont_obj=client.containers.get(hostname)
   return str(cont_obj.top()['Processes'][0][1])


if __name__ == '__main__':	
	app.debug=True
	app.run(host='0.0.0.0', use_reloader=False)
