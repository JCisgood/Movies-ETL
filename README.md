## Project Overview
In this module, we learned how to use the Extract, Transform, Load (ETL) process to create data pipelines. A data pipeline moves data from a source to a destination, and the ETL process creates data pipelines that also transform the data along the way. Analysis is impossible without access to good data, so creating data pipelines is often the first step before any analysis can be performed. Therefore, understanding ETL is an essential skill for data analysis.

## Resources
The Movies-ETL uses Kaggle data. The Kaggle dataset pulls from the MovieLens dataset of over 20 million reviews and contains a metadata file with details about the movies from [The Movie Database (TMDb)](https://www.themoviedb.org/). There, we downloaded [the zip file from Kaggle](https://www.kaggle.com/rounakbanik/the-movies-dataset/download), extracted, and decompressed the CSV files we were interested in; movies_metadata.csv and ratings.csv files. 

- **Data Source:** [wikipedia-movies.json](/data/wikipedia-movies.json), [movies_metadata.csv](/data/movies_metadata.csv), [ratings.csv](/data/ratings(c).xlsb)
- **Software:** Python, Pandas, PostgreSQL  

## Objectives 
- Create an ETL pipeline from raw data to a SQL database.
- Extract data from disparate sources using Python.
- Clean and transform data using Pandas.
- Use regular expressions to parse data and to transform text into numbers.
- Load data with PostgreSQL.

## Summary
### ETL  
The idea behind **ETL** is straightforward. Raw data exists in multiple places and needs to be cleaned and structured before it can be analyzed. ETL breaks this problem into three steps, or phases: **Extract, Transform, and Load.**  

ETL is a flexible process for moving data. It can be as simple as a one-time migration from one database to another, or as complex as an ongoing automated collection of messy, real-time data from many different sources.  

**EXTRACT**  
In the Extract phase, data is pulled from external or internal sources, possibly disparate. The sources could be flat files, scraped webpages in HTML or JavaScript Object Notation (JSON) format, SQL tables, or even streams of sensor data. The extracted data is held in a staging area in between the data sources and data targets.  
**For Movies-ETL, we extracted scraped Wikipedia data stored as a JSON, and Kaggle data stored in CSVs.**  

     
**TRANSFORM**  
After data is extracted, there are many transformations it may need to go through. The data may need to be filtered, parsed, translated, sorted, interpolated, pivoted, summarized, aggregated, merged, or more. The goal is to create a consistent structure in the data. Without a consistent structure in our data, it’s almost impossible to perform any meaningful analysis.  
**We used Python and Pandas to explore, document, and perform our data transformation.**  

     
**LOAD**  
Finally, after the data is transformed into a consistent structure, it’s loaded into the data target. The data target can be a relational database.  
**We loaded our data into a PostgreSQL table.**  
  
<br/>
<br/>
### Iterative Process
## We followed an iterative process based on three key steps: 
  
The iterative process for cleaning data can be broken down as follows:  
- First, we need to inspect our data and identify a problem.
- Once we’ve identified the problem, we need to make a plan and decide whether it is worth the time and effort to fix it.
- Finally, we execute the repair.  

Before we can do anything, we have to look at our data. The first thing we want to know is whether or not the data was imported correctly. The simplest way to confirm this is to print out the first few data points and examine the first few rows for irregularities.  However, most usable data contains too many data points to review every single one, so use strategies that tell us about the whole dataset.  
     <br/>  
     <br/>
After we’ve investigated our data and started to identify problem areas, we can make decisions about how to fix the problems. This requires articulating the problems clearly—even if that is simply expressing the problems to ourselves—and devising a plan to modify the data and fix the problem. With clearly stated steps to fix the problem, we can make an informed decision about whether implementing the plan is worth the effort. Sometimes there are multiple viable resolutions to choose from. To decide, we weigh trade-offs and ultimately choose the best option.  
     <br/>  
     <br/>
Once we have a detailed list of steps to modify our dataset, it’s time to implement it. We’ll start writing code to fix the problem we’re focusing on.  
<br/>  
<br/>
<br/>  

## Challenge Overview <br/>  
While ETL can absolutely be used for a one-time transfer of data, it becomes really powerful when it can be automated as a repeated, ongoing process. Since this process will be running without supervision, it won’t be necessary to perform the exploratory data analysis steps. However, if new incoming data contains errors, the ETL process may halt or produce corrupted data. Adding try-except blocks will make the automated ETL script more robust to errors.

## Objectives
- Create an automated ETL pipeline.
- Extract data from multiple sources.
- Clean and transform the data automatically using Pandas and regular expressions.
- Load new data into PostgreSQL.

## Challenge Summary  
### Create a function

### Transformation Steps
We used the code from our Jupyter Notebook so that the function performs all of the transformation steps, removed any exploratory data analysis, and removed redundant code: [challenge.ipynb](/challenge.ipynb)  

### Load Steps  
Then, we added the load steps from the Jupyter Notebook to the function. We had to remove the existing data from SQL, but keep the empty tables.  

### Check Function
Here, We checked that the function works correctly on the current Wikipedia and Kaggle data using pgAdmin by verifing the columns have the correct data type, inspecting the first 100 rows, and checking the row count.  

<br/>
<br/>
<br/>
<br/>

### Predefined Clean-up Actions
Some objects define standard clean-up actions to be undertaken when the object is no longer needed, regardless of whether or not the operation using the object succeeded or failed. Objects which, like files, provide predefined clean-up actions will indicate this in their documentation.  

> #print out the range of rows that are being imported  

> **data.to_sql(name='ratings', con=engine, if_exists='append')**

When printing out monitoring information, it’s generally a good practice to print out when a process is beginning and when a process has ended successfully, because if there’s any problem, we have a better sense of which process caused the problem by seeing what part never finished successfully.

On top of this, it’s good practice to keep both outputs on the same line, because it’s easier to monitor which step is currently being performed. To do this, we use the end= parameter in the print function. Setting the end to an empty string will prevent the output from going to the next line.
  


## Limitations  
The ratings data is too large to import in one statement, so it has to be divided into “chunks” of data. To do so, we reimported the CSV using the chunksize= parameter in read_csv(). We added functionality to our code to print out:
- How many rows have been imported
- How much time has elapsed  

This is an optional step, but it’s a good idea when running a long process. We’re going to print the total amount of time elapsed at every step. This is useful to estimate how long the process is going to take. As, this can take quite a long time to run (more than an hour).
