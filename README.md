# Cloud-Computing-Rideshare
Through this project, we aim to simulate a typical ride sharing application, where users can create new rides, join existing ones and view details of their booked rides.

Running the project:

In the DbaaS VM:

To run Zookeeper and RabbitMQ
In the home directory:

sudo docker-compose build

sudo docker-compose up 

To run the Orchestrator 
In the orch/app directory:

sudo docker-compose build

sudo docker-compose up

Since the workers are being spawned from the Orchestrator,an image 'slave_app:latest' and a network called 'slave_default' needs to exist. If not, make sure it is created by running 

sudo docker-compose build

sudo docker-compose up

in the orch/app/slave/app directory, and the process can be killed after the network is created. 

In the Users VM:

To run the users container
In the users/app directory:

sudo docker-compose build

sudo docker-compose up


In the Rides VM:

To run the rides container 
In the rides/app directory:

sudo docker-compose build

sudo docker-compose up



