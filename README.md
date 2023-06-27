## Python-and-Cloud-Capstone-Project
#10alytics cloud computing and python data pipeline capstone project solution.
In this project, a data infrastructure solution that provides daily job updates on the 10Alytics job board was built using VScode then configured scheduled, and monitored using airflow.
A pipeline architecture diagram to show the data flow from the source to the destination was designed in the draw.io environment as depicted in Data_pipeline_flowchart.draw.io. The main.py file contains the code for the entire pipeline which can be run from the Apache airflow user interface. 
etl.py contains the DAG representing the extraction, transformation, and loading task.
util.py contains Python functions and database connections.
