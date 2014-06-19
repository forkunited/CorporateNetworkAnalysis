# CorporateNetworkAnalysis #

This repository is part of the Sloan corporate network extraction project.
It contains code for constructing corporate networks from corporate 
relationship type posteriors output by 
*corp.scratch.RunModelTree* from the CorporateRelationExtraction project.
It also code for summarizing and transforming the resulting networks. You 
can get an idea of how things work by reading 
through the other Sloan documentation included in the Sloan project 
distribution (in the tarball, not in this repository), reading through
the CorporateRelationExtraction project documentation, reading through
the current document, and then reading through the description of each 
class located at the top of each code file.

## Layout of the project ##

The code is organized into the following packages in the *src* directory:

*	*corp.net* - Classes representing various network objects (nodes,
edges, documents, sources, etc).

*	*corp.net.hadoop* - Code for constructing and summarizing the 
corporate network using Hadoop.

*	*corp.net.scratch* - Code for reformatting and rearranging the 
output from *corp.net.hadoop* on the local file system.

*	*corp.net.summary* - Classes for computing summary measures on the
network (mainly used by *corp.net.hadoop.HSummarizeCorpNet*).

*	*corp.net.util* - Miscellaneous utilities.

*corp.net.hadoop* and *corp.net.scratch* contain the entry points for the 
code, so you should start by looking at those files first.  

## How to run things ##

Before running anything, you need to configure the project for your local 
setup.  To configure, do the following:

1.  Copy files/build.xml and files/corpnet.properties to the root of the
project.

2.  Fill out the copied corp.properties and build.xml files with the 
appropriate settings by replacing text surrounded by square brackets.

3.  Execute "ant build-jar" to create the corp-net.jar file that's used
for running Hadoop jobs. 

Then, you can run the Hadoop jobs (assuming you're on a Hadoop cluster)
using commands of the form:

```
hadoop jar corp-net.jar corp.net.hadoop.[class name] -D mapred.reduce.tasks=[reduce tasks] [input path] [output path]
```

And you can run the tasks from *corp.net.scratch* using commands of 
the form:

```
ant corp.net.scratch.[class name] -D[arg1]=[value1] -D[arg2]=[value2] ...
```
