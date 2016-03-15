package com.readpeer.util.quora;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
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
//            Indexer id = new Indexer("/Users/sesame/Downloads/indexpath/");
//            id.createIndex();
//            id.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        QuoraSearchClient msa = new QuoraSearchClient("/Users/sesame/Downloads/indexpath/");
        msa.connectDB();
        ResultSet resultSet = msa.readDataBase("072708EC67C498D49461AFC4BA27E4D1");
//        msa.dealObject(resultSet);
        msa.search(resultSet);
        msa.close(resultSet);
    }

    public void search(ResultSet resultSet) {
        try {
            while (resultSet.next()) {
                long nodeid = resultSet.getLong("nodeid");
                String bid = resultSet.getString("bid");
                long pid = resultSet.getLong("pid");
                long start = resultSet.getLong("start");
                long end = resultSet.getLong("end");

                String regExp = "[,\\s]+";
                searcher = new Searcher(indexDir);
                String queryStr = resultSet.getString("keywords").trim().replaceAll("^\\[|\\'|\\]$", "");
                List<String> queryTerms = Arrays.asList(queryStr.split(regExp));
                TopDocs hits = searcher.search(queryTerms);
                System.out.println(hits.totalHits);
                long score = 5;
                for (ScoreDoc scoreDoc : hits.scoreDocs) {
                    Document doc = searcher.getDocument(scoreDoc);
                    String tid = doc.get(LuceneConstants.FILE_NAME);
                    System.out.println("Doc is:"+tid);
                    String result = doc.get(LuceneConstants.CONTENTS);
                    String url = doc.get(LuceneConstants.URL);
                    String question = doc.get(LuceneConstants.QUESTION);
                    writeDataBase(tid, nodeid, bid, pid, url, question, result, score, start, end);
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
        String DB_URL = "jdbc:mysql://172.29.27.171:3306/";
        String USER = "readpeer";
        String PASS = "readpeer";
        Class.forName(JDBC_DRIVER);
        // Setup the connection with the DB
        connect = DriverManager.getConnection(DB_URL, USER, PASS);
    }

    public ResultSet readDataBase(String book_id) {
        ResultSet resultSet = null;
        try {
            preparedStatement = connect.prepareStatement("select * from ivle.keywords where bid = ?");
            preparedStatement.setString(1, book_id);
            resultSet = preparedStatement.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return resultSet;
    }

    public void writeDataBase(String tid, long nodeid, String bid, long pid, String url, String question, String result, long score, long start, long end) {
        try {
            preparedStatement = connect.prepareStatement("insert into tweets_db.quorasfull value (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            preparedStatement.setString(1, tid);
            preparedStatement.setLong(2,  nodeid);
            preparedStatement.setString(3, bid);
            preparedStatement.setLong(4, pid);
            preparedStatement.setString(5, url);
            preparedStatement.setString(6, question);
            preparedStatement.setString(7, result);
            preparedStatement.setLong(8, score);
            preparedStatement.setLong(9, start);
            preparedStatement.setLong(10, end);
            int success = preparedStatement.executeUpdate();
            System.out.println("Writing into database:"+success);

        } catch (SQLException e) {
            e.printStackTrace();
        }


    }

//    public List<RecordObject> dealObject(ResultSet resultSet) throws SQLException{
//        List<RecordObject> listOfObject = new ArrayList<RecordObject>();
//        while(resultSet.next()) {
//            long jobId = resultSet.getLong("job_id");
//            String bookId = resultSet.getString("book_id");
//            String query = resultSet.getString("query");
//            long start = resultSet.getLong("start");
//            long end = resultSet.getLong("end");
//            RecordObject recordObject = new RecordObject(jobId, bookId, query, start, end);
//            listOfObject.add(recordObject);
//        }
//
//        return listOfObject;
//    }

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
