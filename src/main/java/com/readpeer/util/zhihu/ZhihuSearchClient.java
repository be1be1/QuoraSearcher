package com.readpeer.util.zhihu;

/**
 * Created by Beibei on 25/4/2016.
 */

import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.sql.*;
import java.util.*;

public class ZhihuSearchClient {
    private Connection connect = null;
    private Searcher searcher = null;
    private Map<String, Long> dupRemover = null;

    public ZhihuSearchClient(String indexDir) {
        dupRemover = new HashMap<String, Long>();
        try {
            searcher = new Searcher(indexDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void connectSqlDB() throws Exception{
        // This will load the MySQL driver, each DB has its own driver
        String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        String DB_URL = "jdbc:mysql://172.29.27.171:3306/";
        String USER = "readpeer";
        String PASS = "readpeer";
        Class.forName(JDBC_DRIVER);

        connect = DriverManager.getConnection(DB_URL, USER, PASS);
    }

    public void searchAndUpdate(ResultSet resultSet, boolean localDupRemover, String queryType) {

        long globalPid = 0;
        try {
            while (resultSet.next()) {
                long nodeid = resultSet.getLong("nodeid");
                String bid = resultSet.getString("bid");
                long pid = resultSet.getLong("pid");
                long start = resultSet.getLong("start");
                long end = resultSet.getLong("end");


                String queryStr = resultSet.getString("keywords").trim().replaceAll("^\\[|\\'|\\]$", "");
                List<String> queryTerms = Arrays.asList(queryStr.split(","));

                if (localDupRemover) {
                    if (pid != globalPid) {
                        System.out.println("pid is " + pid + " Global pid is " + globalPid + " Page has changed, change the map.");
                        globalPid = pid;
                        dupRemover = new HashMap<String, Long>();
                    }
                }


                TopDocs hits = null;
                if (queryType.equals("booleanQuery")) {

                    hits = searcher.booleanQuerySearch(queryTerms);
//                    System.out.println("Use boolean query");

                } else if (queryType.equals("termQuery")) {

                    hits = searcher.normalQuerySearch(queryTerms);
                    System.out.println("Use term query");

                } else {
                    System.out.println("No such query type provided");
                }

                for (ScoreDoc scoreDoc : hits.scoreDocs) {

                    Document doc = searcher.getDocument(scoreDoc);
                    String fid = doc.get(com.readpeer.util.zhihu.LuceneConstants.FILE_NAME);
                    String question = doc.get(com.readpeer.util.zhihu.LuceneConstants.QUESTION);
                    String result = doc.get(com.readpeer.util.zhihu.LuceneConstants.CONTENTS);
                    String url = doc.get(com.readpeer.util.zhihu.LuceneConstants.URL);
                    double score = scoreDoc.score;

                    int cont = 5;

                    if (!dupRemover.containsKey(fid) && cont > 0) {
                        dupRemover.put(fid, nodeid);
                        cont--;

                        writeDataBase(fid, nodeid, bid, pid, url, question, result, score, start, end);
                    }
                }
            }
            searcher.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public ResultSet readDataBase(String book_id) {
        ResultSet resultSet = null;

        try {
            PreparedStatement preparedStatement = connect.prepareStatement("select * from ivle.keywords where bid = ?");
            preparedStatement.setString(1, book_id);
            resultSet = preparedStatement.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return resultSet;
    }

    public void writeDataBase(String fid, long nodeid, String bid, long pid, String url,
                              String question, String result, double score, long start, long end) {
        try {
            PreparedStatement preparedStatement = connect.prepareStatement("insert into tweets_db.zhihufull value (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            preparedStatement.setString(1, fid);
            preparedStatement.setLong(2,  nodeid);
            preparedStatement.setString(3, bid);
            preparedStatement.setLong(4, pid);
            preparedStatement.setString(5, url);
            preparedStatement.setString(6, question);
            preparedStatement.setString(7, result);
            preparedStatement.setDouble(8, score);
            preparedStatement.setLong(9, start);
            preparedStatement.setLong(10, end);
            preparedStatement.setDouble(11, 0.0);
            preparedStatement.setDouble(12, 0.0);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void close(ResultSet resultSet) {
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