BasicStorageServer
==================

Milestone 3

Use instructions:

-	First, launch ECS jar in a console:
o	 Go to BasicStorageServer folder
o	Type “java –jar ms3-ecs.jar DEBUG”
o	“init 2 5 FIFO”
o	“start”
o	… whatever you want! (“addnode 5 FIFO”/ “removenode” / “stop”/”shutdown”)

-	Then, launch client jar in a console:
o	Go to BasicStorageServer folder
o	Type “java –jar ms3-client.jar”
o	... whatever you want! (“put foo bar” / “get foo” / …)

- If you want to try using the system with distant nodes
(ip of server different from "localhost"), as SSH is not working currently,
before launching ECS and client,
you have to launch 1 server jar per node you have declared in the ecs.config file: 
Type "java -jar ms3-server.jar <portnumber> DEBUG" in a console ON THE DISTANT MACHINE
and comment the line "launchSSH(...)" in the addNodeToLists method of ECSClient.java

-	Troubleshooting:
o	If the servers are not responding, check that the path to the server jar is correct in the java code
(method “launchSSH” in ECSClient.java) (Clotilde -> nadiastraton) 
o	If you modified the java code and the change does not seem to be taken into account,
check that you re-built the code (running the build.xml) before launching the jars
o	If the build fails, check that every jar was exited in the consoles ("quit" or if not working, CTRL+C)
(cannot update the jar files if they are still in use) 


-	Latency of 1000 writes to n servers  (without cache, with FIFO cache 50, with FIFO cache 1000/n)
o	Mean time of a write to memory / to cache?
o	Try many values of n (put in evidence the overhead of writing to the last line of a looooong file in memory. More servers: shorter file.)
-	Latency of 1000 writes to n servers  with concurrency: k clients (without cache, with FIFO cache 50, with FIFO cache 1000/n)
o	Mean time of a write to memory / to cache?
o	Try many values of k (put in evidence the influence of serving many clients at a time).
-	Latency of 100 random reads to n servers  (without cache, with FIFO cache 20)
o	Write 1000 KV to store
o	Prepare list of keys to read (pick 100 random keys among the 1000)
o	Read them
o	Mean time of a read?
o	Try many values of n (put in evidence the overhead of retrieving a line of a looong file in memory. More servers: shorter file.)
-	Latency of 100 reads to n servers  with concurrency: k clients (without cache, with FIFO cache 50, with FIFO cache 1000/n)
o	Mean time of a read?
o	Try many values of k (put in evidence the influence of serving many clients at a time).
-	Latency of adding a node
o	Init with n nodes
o	Write 1000 KV to store
o	Add some node
o	Regarding the size of the interval of responsibility, is it coherent (number of KV to move)?
-	Same for removing a node



