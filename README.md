# cascading-jdbc-on-hdp
Sample project for running Cascading-jdbc on Hortonworks

# Installation and Deployment


# Getting started


* Make sure that small.txt is available in HFS, the current implementation looks for the file in the current users directory, which in my case is <code>/user/root</code>
* start a postgres database somewhere, I was using docker as follows: 
	<code>docker run -P -e POSTGRES_PASSWORD=password --rm postgres</code>
	which resulted in a container running on 172.16.102.85 listening at port 32768 (see TestJdbc1.java) 
* If not done, deploy the packaged project to the Hortonworks Sandbox
  * Either run <code>gradle assemble copyAllDependencies</code>  and copy the contents of build/libs to the server, or
  * Update ext.targetDir (approx. line 53 in build.gradle), create the directory on the server once, and run gradle uploadToHortonworksSandbox 
* Put the job to yarn, using 

yarn jar cascading-jdbc-on-hdp2.jar net.brunel.TestJdbc1
 
