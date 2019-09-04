1.-The purpose of this design is to store the information of the Sparkify application so that they can obtain information about the preferences of their users for a certain song, time of duration in which the song was heard, season, schedule, etc.
2.-The star model was used, creating 4 dimensional tables and a fact table that stores the keys and common information of the tables with which it is related

Steps to excute the scripts:
1.-Configure 'dwh.cfg' to put aws credentials for AWS cluster
2.-Agregate the logic necesary for load the tables songplays,users,songs,artists,time.This logic will be in spark.
3.-Execute 'etl.py' to load data into file in s3 of the tables songplays,users,songs,artists,time with information of the before step (3).
4.-For execute to consola writting 'python etl.py',then verify in s3 file the load of the tables.