Table of Contents
-----------------

1. Directory structure of the WorkflowSim Toolkit
2. Software requirements: Java version 1.6 or newer 
3. Installation and running the WorkflowSim Toolkit


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
### Welcome to WorkflowSim Pages.
WorkflowSim is a workflow simulator to support large-scale scheduling, clustering and provisioning studies. It is developed by Weiwei Chen, a Phd student from University of Southern California under the Apache License version 2.0. WorkflowSim is not yet fully completed and we welcome your contribution to this project. 

This page introduces the basic features of WorkflowSim and how to install and run it with Eclipse or NetBeans.

### 1. Use WorkflowSim with GitHub/Eclipse

( It is suggested to use WorkflowSim if you are going to contribute back to this project, otherwise it is not required. )

We provide a youtube video to show the process described below. (http://www.youtube.com/watch?v=MvpPzZpMDzg)

1.1 Register a GitHub account and fork your own branch

Go to the repository page (https://github.com/WorkflowSim/WorkflowSim-1.0) and click 'Fork' on the top-right corner next to 'Star'. Then you will have your own branch of WorkflowSim and you can maintain your codes under this branch and commit your changes to it. In the video, we use 'chenww05' as an example and you will see a new repo called chenww05/WorkflowSim-1.0. Go to your repo, and copy the path. In this case, it is "https://github.com/chenww05/WorkflowSim-1.0.git". 

1.2 Install EGit


Open your Eclipse, go to 'Help'->'Install New Software', in 'Work with', choose '--All Available Sites', make sure you have EGit listed, otherwise click the item and install it. 

1.3 Check out your source codes

First, create a new java project called 'WorkflowSim' (it doesn't have to be 'WorkflowSim'). Right click your project, choose 'Import'. Click 'Git'->'Projects from Git'. Set the URL to be "https://github.com/chenww05/WorkflowSim-1.0.git". And then you will see a branch called 'master', tick it and continue. And you don't need import projects again so just cancel. 

1.4 Import Source Files and Libraries

Right click the project again and choose 'Properties'. Go to 'Java Build Path'. Link two source directories (your_repo_root/examples, your_repo_root/sources)to the source folders. Add two external JARs (your_repo_root/lib/flanagan.jar and your_repo_root/lib/jdom-2.0.0.jar) to the Libraries.  

1.5 Set Main class and arguments

Go to 'Run'->'Run Configurations'->'Java Application'->'WorkflowSimExample1'. Make sure the Main class is 'org.workflowsim.examples.WorkflowSimExample1' although by default Eclipse should set the main class for you automatically. 

Click the 'Arguments', in the 'Program arguments', please add
```
-p your_repo_root/config/config.txt
```
In this case, we use Montage workflow as an example. 

1.6 Modify Config File

Open your_repo_root/config/config.txt, there is one line:
```
dax.path	= /Users/chenweiwei/Work/WorkflowSimEnergy/WorkflowSim-1.0/config/dax/Montage_100.xml
```
Replace it with 
```
dax.path = your_repo_root/config/dax/Montage_100.xml. 
```
Montage_100.xml is a dax file for montage workflow with 100 tasks in it. You can try other dax files provided under the same directory. 
Other parameters would not cause an error but they have specified the simulation details. For details of these parameters, please refer to other pages. 

1.7 Run an Example

After you have done all the changes, you can click 'Run' and the output contains the completion time of each job (if you use task clustering) or each task. 

### 2. Use WorkflowSim with GitHub/NetBeans

The youtube video for install WorkflowSim with Git/NetBeans is here: http://www.youtube.com/watch?v=G94FW2DZbj0

2.1 Register a GitHub account and fork your own branch

It is the same to 1.1.

2.2 Install Git

Go to 'Teams'->'Available Plugins'. Search for 'Git' and install it. 

2.3 Check out your source codes

First, create a new java project with Existing Sources called 'WorkflowSim' (it doesn't have to be 'WorkflowSim'). Right click this project and choose 'Set as Main Project' for convenience. 
Right click this project again and choose 'Versioning'->'Initialize Git Repository'. You don't need to change the root path, just click 'OK'. 

Right click the project and choose 'Git'->'Remote'->'Pull', set the 'Repository URL' to be 'https://github.com/chenww05/WorkflowSim-1.0.git' and the 'Remote name' to be 'master'. Click 'OK' and you will see a branch called 'master', tick it and continue. 

2.4 Import Source Files and Libraries

After a while the download is down, right click the project again and choose 'Project Properties'. Go to 'Sources', click 'Add Folder' and choose two folders (your_repo_root/examples and your_repo_root/sources). Go to 'Libraries', add all jars  (your_repo_root/lib/*.jar). 

2.5 Set Arguments

Go to 'Run'->'Set Project Configuration'->'Customize'. Go to 'Run', and make sure the Main Class is 'org.workflowsim.examples.WorkflowSimExample1'. Set the 'Arguments' to be '-p your_repo_root/config/config.txt'. 

2.6 Modify Config File

The same to 1.6. 

2.7 Run an Example

The same to 1.7. 

### 3. Use WorkflowSim with Eclipse but without GitHub
( If you don't want to contribute back to WorkflowSim with your codes, you can use WorkflowSim without GitHub. )

3.1 Download Source Files. 

Skip 1.1 and 1.2. Different to 1.3, we download source files directly from https://github.com/WorkflowSim/WorkflowSim-1.0/archive/master.zip and unzip it to your_repo_root. 

3.2 Switch to 1.4 and continue with the rest steps (1.5-1.7). 

### 4. Use WorkflowSim with NetBeans but without GitHub

4.1 Download Source Files

The same to 3.1. 

4.2 Switch to 2.4 and continue with the rest steps (2.5-2.7).

### Authors and Contributors
This page is written by Weiwei Chen @chenww05. For details or bug reports, please contact the author. 

### Support or Contact
Please send an email to support@workflowsim.org. We appreciate your contribution to this project and please go to github to submit your bug report.  

### Mailing Lists

WorkflowSim Announce
Message about new release or updates
workflowsim-announce@googlegroups.com

WorkflowSim Users
Messages about WorkflowSim related questions/reports
workflowsim-user@googlegroups.com

WorkflowSim Developers
Messages about WorkflowSim development
workflowsim-dev@googlegroups.com



