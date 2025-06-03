# Data-Science-Piscine

The goal if this piscine is to get exposed to different challenges in the data field according to the role: Data Engineer, Data Warehouse, Data Scientist.

## Day 1
In the first day the challenge is to create a PostgreSQL Database and populate it with data stored in a csv file, these csv files are big and saving those files into the database without thinking in an optimization strategy for saving the data, may be computationally expensive 😿 (this is one of the main challenges of the exercise.)
For the script I used python since i want to get in touch again with it after long time, another powerfull option to consider is to use a bash script 🚀 and process the data in chunks of x amount of rows, this should do the work 😉

The setup used to develop this project is a Docker Compose in which we have 2 services: a PostgreSQL Database and a pgAdmin to use as front-end of our Database and query the data. 

To build the project run `make up` be aware that you may need to create your own environment variables and put the csv files to process and load into your folder before running the make up command.

After building the image the frontend should look like this:
![alt text](image.png)
From here, we connect the database and then it should be ready to create tables on it to store and retrieve data as needed. 

## Day 2
In the second day the challenge is to build a pipeline ETL process in which we need to join the previously loaded csv files and merge then into a single file considering removing the duplicates values.
