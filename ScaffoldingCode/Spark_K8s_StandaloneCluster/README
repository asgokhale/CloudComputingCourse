Author: Aniruddha Gokhale
Vanderbilt University
Created: Spring 2021

	 Purpose:
	 --------
	 
Show K8s deployment of the wordcount Python example supplied by the Spark distribution.
Additionally show all the Kubernetes YAML files and docker file needed for running
Spark in K8s

	 Directory contents:
	 -------------------

spark-master-svc.yaml		Service declaration of the Spark master
spark-master-deploy.yaml	Deployment declaraiton of the Spark master

spark-driver-svc.yaml		Service declaration of the Spark driver
spark-driver-deploy.yaml	Deployment declaraiton of the Spark driver

spark-worker-deploy.yaml	Deployment declaraiton of the Spark worker

spark-env.sh			Setting of environment vars when Spark runs
spark-worker.conf		Spark worker properties
spark-driver.conf		Spark driver properties

spark_dockerfile		Used to build docker image for Spark

run_iters.sh			Used to run the driver logic from inside
				the driver pod

alice_in_wonderland.txt		The text file for wordcount


	Setup
	-----
We are assuming two virtual machines in which we will set up our K8s cluster.
One of the VMs is the K8s master and will also run the Spark master and driver
pods. Spark workers can (and will) be scheduled on either of the VMs.

Ports used:

* Spark master listens on port 7077
* Spark master dashboard on port 8080, exposed to outside world at port 30008
in nodePort declaration.

* Spark driver listens on port 7076

* Spark worker listens on port 7078
* spark worker dashboard on port 8081 (not exposed to outside world)

* Spark block manager listens on port 7079

Accordingly, all these ports must be allowed on your UFW firewall and in security groups.

      Preparatory Steps
      -----------------
A common docker image is needed for which a dockerfile is provided. Ensure that
the image is pushed into a private registry or on your hub.docker.com account

e.g.,
docker build -f spark_dockerfile -t my-spark .
docker tag my-spark <priv reg IP addr:port>/my-spark
docker push <priv reg IP addr:port>/my-spark

Ensure security groups and ufw rules are set for the two VMs.

       Execution
       ---------

(1) First start the services for the spark master and driver on the K8s master node.

    kubectl apply -f spark-master-svc.yaml
    kubectl apply -f spark-driver-svc.yaml

(2) Check and ensure that all services are registered

    kubectl get svc -o wide


(3) Now start the deployments in this order

    kubectl apply -f spark-master-deploy.yaml

after a short pause,

    kubectl apply -f spark-worker-deploy.yaml
    
    kubectl apply -f spark-driver-deploy.yaml

(4) Check and ensure that all pods are deployed

    kubectl get pods -o wide

(5) Exec into the driver pod using its displayed id

    kubectl exec -it <driver pod full instance id> -- /usr/bin/bash

once inside, then issue the following long command to run the Wordcount example
(a backslash shown below indicates continuation of the line)

     ${SPARK_HOME}/bin/spark-submit --master spark://spark-master-svc \
     --properties-file ${SPARK_HOME}/conf/spark-driver.conf \
     ${SPARK_HOME}/examples/src/main/python/wordcount.py \
     /alice_in_wonderland.txt






