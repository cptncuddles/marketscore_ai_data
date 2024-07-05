# Marketscore ELT Pipeline
---
## Required Libraries
- Pandas
- Json
- Glob
- Time *(This can be dropped if you don't want to track performance)*
---
## Summary
Leveraging knowledge of Pandas and Python I was tasked with setting up a pipeline and maintaining a
data warehouse for future use by a generative AI in the use of stock day trading. This required extensive knowledge of data
cleaning and manipulation. This project is ongoing with the most current iteration moving away from a RDD with Spark and into
a **SQL** database utilizing **Pandas** for data wrangling and cleaning.

### MSCOUNT Converter
This program is designed take in files from the disk and process them through Pandas, organizing them into the first of three
relational databases that are all related through the unique *timestamp* and *date*. This database organizes all of the tracked 
stock ticker symbols keeping an aggreagate of all of the calls and puts that have occured at that given timestamp.

### Ticker Converter
This program is similar to the previous **MSCOUNT Converter** program, intaking a batch of files on the disk and processing them through Pandas. 
This database tracks specific ticker symbols data points needed for future analysis in the generative AI. Specifically tracking the exact number
of calls and puts for each ticker symbol. These data points are *aggregated* to form a score that is used in the final converter. **It's important to
note that this can be used for any ticker symbol and is not specific to any single one.**

### Experimental Marketscore Converter
This program is the most extensive as far as data wrangling and cleaning is concerned. The raw JSON was not very easily manipulated as before, so many
modifications and helper functions needed to be created to assist in the cleaning. Once the file is imported the cleaning process begins by breaking 
down the file contents into smaller more manageable parts. Once everything is partitioned into smaller chunks and sorted into holding lists, they can
be passed to helper functions to be cleaned and made into smaller individual data frames. Two of the helper functions work very similar to the **Ticker
Converter** program and **MSCOUNT Converter**: cleaning and processing out the data while *creating a common field* to allow for the merging of the seperate
databases back together. One helper function, **meta_data_cleaner**, did a majority of the work:
1. Taking the passed list of dictionaries
2. Creating an aggregate key : value pair (using two fields and creating a tuple pair of two values for easier tracking and scoring)
3. Passing that into the dictionary to then be created into a data frame.
  
Once the helper functions process and return the separate dataframes they are ready to be merged on the newly created field 'counts'. 
Following the merging this *temporary* field is dropped and the file is processed for export to disk.

**All of these scripts will further be optimized to ingest from a designated API and to export to a remote datawarehouse for long term storage and for future
analysis/use by whichever AI model the Product Owner decides upon.**[^1]

[^1]: The first two files: **Data Cleaning** and **Data Cleaning Report** were the first iterations of this project and will *not* be used in final production.


