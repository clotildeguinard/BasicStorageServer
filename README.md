BasicStorageServer
==================

Milestone 5

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


-	Troubleshooting:
o	If you modified the java code and the change does not seem to be taken into account,
check that you re-built the code (running the build.xml) before launching the jars
o	If the build fails, check that every jar was exited in the consoles ("quit" or if not working, CTRL+C)
(cannot update the jar files if they are still in use)