Table of Contents
-----------------

1. Directory structure of the WorkflowSim Toolkit
2. Software requirements: Java version 1.6 or newer 
3. Installation and running the WorkflowSim Toolkit
4. Running the WorkflowSim examples
5. Learning WorkflowSim
6. Compiling WorkflowSim: using Ant



1. Directory structure of the WorkflowSim Toolkit
----------------------------------------------

workflowsim/			-- top level WorkflowSim directory
	docs/			-- WorkflowSim API Documentation
	examples/		-- WorkflowSim examples
	lib/			-- WorkflowSim jar archives
	sources/		-- WorkflowSim source code


2. Software requirements: Java version 1.6 or newer
---------------------------------------------------

WorkflowSim has been tested and ran on Sun's Java version 1.6.0 or newer.
If you have non-Sun Java version, such as gcj or J++, they may not be compatible.
You can use Eclipse, NetBeans, or Ant to compile and run WorkflowSim

3. Installation and running the WorkflowSim Toolkit
------------------------------------------------

You just need to unpack the WorkflowSim file to install. 

4. Running the WorkflowSim examples
--------------------------------

First, checkout source codes from https://github.com/WorkflowSim/WorkflowSim-1.0. Second, create a java project with existing source codes in Eclipse or NetBeans. After that, set the main class to be examples.org.workflowsim.examples.WorkflowExample1.java and in the configuration please specify at least -p $WORKFLOWSIM/config/balanced/cybershake.txt
In $WORKFLOWSIM/config/balanced/cybershake.txt, please replace dax.path to be the real path to a dax file, such as 
dax.file=$WORKFLOWSIM/config/balanced/dax/Inspiral_1000.xml
Now you can run it. 

We have youtube videos to ease your installation.


5. Learning WorkflowSim
--------------------

To understand how to use WorkflowSim, please go through the examples provided
in the examples/ directory.


6. Compiling WorkflowSim: using Ant
--------------------------------

  
  
 7. Downloading and using external jars
---------------------------------------



