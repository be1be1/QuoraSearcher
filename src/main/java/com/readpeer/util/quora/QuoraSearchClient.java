package com.readpeer.util.quora;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.sql.*;
import java.util.*;

public class QuoraSearchClient {
    private String indexDir = null;
    private Connection connect = null;
    private PreparedStatement preparedStatement = null;
    private Searcher searcher = null;
    private Map<String, Long> dupRemover = null;

    public QuoraSearchClient(String indexDir) {
        this.indexDir = indexDir;
        dupRemover = new HashMap<String, Long>();
        try {
            searcher = new Searcher(indexDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
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
        ResultSet resultSet = msa.readDataBase("1711");
        msa.search(resultSet);
        msa.close(resultSet);
//        TfIdfScore idf = new TfIdfScore("/Users/sesame/Downloads/indexpath/");
//        double result = idf.getIdfScore("shit");
//        System.out.println("The result is: "+result);
    }

    public void search(ResultSet resultSet) {
        long globalPid = 0;
        try {
            while (resultSet.next()) {
                long nodeid = resultSet.getLong("nodeid");
                String bid = resultSet.getString("bid");
                long pid = resultSet.getLong("pid");
                long start = resultSet.getLong("start");
                long end = resultSet.getLong("end");
                String label = resultSet.getString("label");

                String queryStr = resultSet.getString("keywords").trim().replaceAll("^\\[|\\'|\\]$", "");
                List<String> queryTerms = Arrays.asList(queryStr.split(","));

//                if(pid != globalPid) {
//                    globalPid = pid;
//                    dupRemover = new HashMap<String, Long>();
//                    System.out.println("pid is "+pid+" Global pid is "+globalPid+" Page has changed, change the map.");
//                }

                for(String queryTerm:queryTerms) {
                    System.out.print(queryTerm+",");
                }

                List<String> globalQueryTerms = new ArrayList<String>();
                List<String> localQueryTerms = new ArrayList<String>();

                String[] labelArray = label.split(" ");

                for(int i = 0; i< labelArray.length-1; i++) {
                    if(labelArray[i].toString().equals("1")) {
                        globalQueryTerms.add(queryTerms.get(i));
                    }

                    if(labelArray[i].toString().equals("0")) {

                        localQueryTerms.add(queryTerms.get(i));
                    }
                }

                System.out.println();
                TopDocs hits = searcher.search(queryTerms);

                for (ScoreDoc scoreDoc : hits.scoreDocs) {
                    Document doc = searcher.getDocument(scoreDoc);
                    String tid = doc.get(LuceneConstants.FILE_NAME);
                    String result = doc.get(LuceneConstants.CONTENTS);
                    String url = doc.get(LuceneConstants.URL);
                    String question = doc.get(LuceneConstants.QUESTION);
                    int cont = 5;

                    if(!dupRemover.containsKey(tid) && scoreDoc.score >= 0.5 && cont > 0) {
                        dupRemover.put(tid, pid);
                        cont--;
                        double globalScore = getScore(question, result, globalQueryTerms);
                        double localScore = getScore(question, result, localQueryTerms);
                        System.out.println(scoreDoc.score);
                        writeDataBase(tid, nodeid, bid, pid, url, question, result, globalScore, localScore, start, end);
                    }
                }
//                Page Filtering
//                double score = 0.0;
//                int cont = 5;
//                for (ScoreDoc scoreDoc : hits.scoreDocs) {
//                    Document doc = searcher.getDocument(scoreDoc);
//                    String tid = doc.get(LuceneConstants.FILE_NAME);
//                    String result = doc.get(LuceneConstants.CONTENTS);
//                    String url = doc.get(LuceneConstants.URL);
//                    String question = doc.get(LuceneConstants.QUESTION);
//
//                    if(!dupRemover.containsKey(tid) && cont > 0) {
//                        dupRemover.put(tid, pid);
//                        cont--;
//                        System.out.println("Page: "+pid+" document returned: "+tid);
//                        double globalScore = getScore(question, result, globalQueryTerms);
//                        double localScore = getScore(question, result, localQueryTerms);
//                        System.out.println(scoreDoc.score);
//                        writeDataBase(tid, nodeid, bid, pid, url, question, result, globalScore, localScore, start, end);
//                    }
//
//                    score--;
//                }
                searcher.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public double getScore(String question, String result, List<String> queryTerms) {

        double total = 0;
        double num = 0;
        for (String queryTerm : queryTerms) {

            System.out.println("Query term is: " + queryTerm);
            total = total + 2.0;
            if (question.toLowerCase().contains(queryTerm)) {
                num = num + 1.0;
            }

            if (result.toLowerCase().contains(queryTerm)) {
                num = num + 1.0;
            }
        }
        System.out.println(total);
        double freq = num/total;
        return freq;
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

    public void writeDataBase(String tid, long nodeid, String bid, long pid, String url, String question, String result, double globalScore, double localScore, long start, long end) {
        try {
            preparedStatement = connect.prepareStatement("insert into tweets_db.quorasfull value (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            preparedStatement.setString(1, tid);
            preparedStatement.setLong(2,  nodeid);
            preparedStatement.setString(3, bid);
            preparedStatement.setLong(4, pid);
            preparedStatement.setString(5, url);
            preparedStatement.setString(6, question);
            preparedStatement.setString(7, result);
            preparedStatement.setDouble(8, globalScore);
            preparedStatement.setDouble(9, localScore);
            preparedStatement.setLong(10, start);
            preparedStatement.setLong(11, end);
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
