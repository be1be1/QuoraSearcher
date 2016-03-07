# Quora Searcher

This is a java based maven project of quora Indexing and searching application. The keywords are needed for searching purpose. In order to compatible with the readpeer application, keywords and results are read and written respectively.

## Dependencies

1. mongodb driver 3.0.4: The quora cawler is based on mongodb. So, if you want to run indexing, you will find that it fetch the data from mongodb, and then index them. So, mongodb driver are needed.

2. apache lucene core 3.6.2: Apache lucene is used for searching purpose. It utilised the inverted index and it will list the related contents based on the similarity.

3. mysql connector java 5.1.31: the job and result are inside of the mysql database.

## Program Structure

1. You can index the contents by creating a Indexer object after which you could simply call **createIndex()** function. This indexer will not keep indexing if the data of mongodb keep increasing. So, you need to timely re-run the Indexer for indexing more contents of the mongodb.

2. After Indexing, you could take a look at QuoraSearchClient.java. When you initialize this class, you need to specify the indexing path which should be the same as the path you specified inside of the Indexer class. First you need to connect to the database by calling **connectDB()** this will connect to the tweets_db. After that, you can call **readDataBase(String book_id)** to read the related field of a book from **job** table. Bear in mind that you need to provide a **book_id** in order to fetch the query keywords, **job_id** and so on. Calling writeDataBase will write the data to **quorasfull** table. And overall, don't forget to close the resultSet and query by calling **close(ResultSet resultSet)** function.

3. **Searcher** is a class for constructing and actually doing search.  