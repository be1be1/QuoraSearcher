package com.sesame.searcher;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import java.sql.*;
import java.util.Arrays;
import java.util.List;

/**
 * Created by sesame on 4/3/16.
 */
public class QuoraSearchClient {
    private String indexDir = null;
    private Connection connect = null;
    private PreparedStatement preparedStatement = null;
    private Searcher searcher = null;

    public QuoraSearchClient(String indexDir) {
        this.indexDir = indexDir;

    }

    public static void main(String[] args) throws Exception{
//        try {
//            Indexer id = new Indexer(indexDir);
//            id.createIndex();
//            id.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        QuoraSearchClient msa = new QuoraSearchClient("/Users/sesame/Downloads/indexpath/");
        msa.connectDB();
        ResultSet resultSet = msa.readDataBase("7f6fe8e8-cf4d-4b7f-bcc7-8e8515666107");
        msa.search(resultSet);
        msa.close(resultSet);
    }

    public void search(ResultSet resultSet) {
        try {
            while (resultSet.next()) {
                long job_id = resultSet.getLong("job_id");
                String book_id = resultSet.getString("book_id");
                long start = resultSet.getLong("start");
                long end = resultSet.getLong("end");

                String regExp = "[,\\s]+";
                searcher = new Searcher(indexDir);
                String queryStr = resultSet.getString("query").trim().replaceAll("^\\[|\\'|\\]$", "");
                List<String> queryTerms = Arrays.asList(queryStr.split(regExp));
                TopDocs hits = searcher.search(queryTerms);
                System.out.println(hits.totalHits);
                long score = 20;
                for (ScoreDoc scoreDoc : hits.scoreDocs) {
                    Document doc = searcher.getDocument(scoreDoc);
                    String tid = doc.get(LuceneConstants.FILE_NAME);
                    String result = doc.get(LuceneConstants.CONTENTS);
                    String url = doc.get(LuceneConstants.URL);
                    writeDataBase(tid, job_id, book_id, url, result, score, start, end);
                    score--;
                }
                searcher.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void connectDB() throws Exception{
        // This will load the MySQL driver, each DB has its own driver
        String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        String DB_URL = "jdbc:mysql://172.29.25.182:3306/tweets_db";
        String USER = "readpeer";
        String PASS = "readpeer";
        Class.forName(JDBC_DRIVER);
        // Setup the connection with the DB
        connect = DriverManager.getConnection(DB_URL, USER, PASS);
    }

    public ResultSet readDataBase(String book_id) {
        ResultSet resultSet = null;
        try {
            preparedStatement = connect.prepareStatement("select * from job where book_id = ?");
            preparedStatement.setString(1, book_id);
            resultSet = preparedStatement.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return resultSet;
    }

    public void writeDataBase(String tid, long job_id, String book_id, String url, String result,long score, long start, long end) {
        try {
            preparedStatement = connect.prepareStatement("insert into quorasfull value (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            preparedStatement.setString(1, tid);
            preparedStatement.setLong(2,  job_id);
            preparedStatement.setString(3, book_id);
            preparedStatement.setString(4, url);
            preparedStatement.setString(5, result);
            preparedStatement.setLong(6, score);
            preparedStatement.setLong(7, start);
            preparedStatement.setLong(8, end);
            preparedStatement.setTimestamp(9, new Timestamp(System.currentTimeMillis()));
            preparedStatement.setTimestamp(10, new Timestamp(System.currentTimeMillis()));
            int success = preparedStatement.executeUpdate();
            System.out.println("Writing into database:"+success);

        } catch (SQLException e) {
            e.printStackTrace();
        }


    }

    private void close(ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }

            if (connect != null) {
                connect.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
