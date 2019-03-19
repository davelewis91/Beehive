## The BeeHive

#### Simplifying HDFS and Spark interactions in python

Aims to bring file directory structure to Hadoop file paths, through the ```hdfs``` python package. Also provides a
neat API for saving/loading spark tables from the source database to a user area. This includes creating user-specific
directories and using each user's directory as the base for saving tables, reducing risk of overwriting each others'
tables in the wild west that is Hive.

#### Code structure

* QueenBee class: provides the wrapper around the HDFS package, emulating linux commands and relative file paths
* WorkerBee class: handles saving/loading spark tables gracefully by going directly to the underlying parquet files
* CollectorBee class: gives summary statistics for e.g. file sizes of tables in a database (to be expanded upon)

#### Examples

The beehive_example.py file shows how these classes can be used as part of an analytical project

#### Unit tests

Some unit tests are available in the tests/ folder - never been able to get 100% coverage as I have no idea how to get
a docker container to point to our hadoop cluster, as part of a CI pipeline...

#### Package versions

Built in python 2.7, but with 3.6 in mind. Everything bar the __future__ imports should be forward-compatible.

For other requirements, see the pip requirements.txt file!