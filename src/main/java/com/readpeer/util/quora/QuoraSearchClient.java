package com.readpeer.util.quora;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Date;

public class QuoraSearchClient {
    private Connection connect = null;
    private Searcher searcher = null;
    private Map<String, Long> dupRemover = null;

    public QuoraSearchClient(String indexDir) {
        dupRemover = new HashMap<String, Long>();
        try {
            searcher = new Searcher(indexDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void searchAndUpdate(ResultSet resultSet, boolean localDupRemover, String queryType, String contentType) {

        long globalPid = 0;
        double localScore = 0.0;
        double globalScore = 0.0;
        try {
            while (resultSet.next()) {
                localScore = 0.0;
                for (int i = 0; i < 5; i++) {
                    long nodeid = resultSet.getLong("nodeid");
                    String label = resultSet.getString("label");
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

                    localScore = localScore + 0.2;
                    globalScore = 1 - localScore;

                    List<String> globalQueryTerms = new ArrayList<String>();
                    List<String> localQueryTerms = new ArrayList<String>();

                    if(label != null) {
                        String[] labelArray = label.split(" ");

                        for (int k = 0; k < labelArray.length - 1; k++) {

                            if (labelArray[k].toString().equals("1")) {
                                globalQueryTerms.add(queryTerms.get(k));
                            }

                            if (labelArray[k].toString().equals("0")) {
                                localQueryTerms.add(queryTerms.get(k));
                            }
                        }
                    }

                    int localLength = (int) Math.round(localQueryTerms.size() * localScore);
                    int globalLength = (int) Math.round(globalQueryTerms.size() * globalScore);


                    System.out.println("localScore" + localScore + "Original size:" + queryTerms.size() +"now globalSize:"+globalLength+" now localSize:" + localLength);
                    List<String> realGlobalQueryTerms = globalQueryTerms.subList(0, globalLength);
                    List<String> realLocalQueryTerms = localQueryTerms.subList(0, localLength);

                    List<String> realQueryTerms = new ArrayList<String>(localQueryTerms);
                    realQueryTerms.addAll(globalQueryTerms);


                    TopDocs hits = null;
                    if (queryType.equals("booleanQuery")) {

                        hits = searcher.booleanQuerySearch(realQueryTerms);
//                    System.out.println("Use boolean query");

                    } else if (queryType.equals("termQuery")) {

                        hits = searcher.normalQuerySearch(realQueryTerms, contentType);
//                    System.out.println("Use term query");

                    } else {
                        System.out.println("No such query type provided");
                    }

                    List<String> storedTexts = new ArrayList<String>();

                    for (ScoreDoc scoreDoc : hits.scoreDocs) {
                        if (contentType.equals("quora")) {
                            Document doc = searcher.getDocument(scoreDoc);
                            String fid = doc.get(LuceneConstants.FILE_NAME);
                            String question = doc.get(LuceneConstants.QUESTION);
                            String result = doc.get(LuceneConstants.CONTENTS);
                            String url = doc.get(LuceneConstants.URL);
                            double score = scoreDoc.score;

                            int cont = 5;

                            if (!dupRemover.containsKey(fid) && cont > 0) {
                                dupRemover.put(fid, nodeid);
                                cont--;

                                writeDataBase(fid, nodeid, bid, pid, url, question, result, score, start, end, globalScore, localScore);
                            }
                        }

                        if (contentType.equals("tweets")) {
                            boolean shouldStore = true;
                            Document doc = searcher.getDocument(scoreDoc);
                            String tid = doc.get("tid");
                            String text = doc.get("text");
                            double score = scoreDoc.score;

                            for (String storedText : storedTexts) {

                                if (StringSimilarityUtil.similarity(text, storedText) > 0.7) {
                                    System.out.println("Similarity is high: " + storedText);
                                    shouldStore = false;
                                    break;
                                }

                                if (text.equalsIgnoreCase(storedText)) {
                                    System.out.println("Exactly the same" + storedText);
                                    shouldStore = false;
                                    break;
                                }
                            }

                            if (shouldStore) {
                                storedTexts.add(text);
                                writeDataBase(tid, text, score, bid, pid, start, end);
                            }
                        }
                    }
                }
            }
            searcher.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public double getScore(String question, String result, List<String> queryTerms) {

        double total = 0;
        double num = 0;
        for (String queryTerm : queryTerms) {

            total = total + 2.0;
            if (question.toLowerCase().contains(queryTerm)) {
                num = num + 1.0;
            }

            if (result.toLowerCase().contains(queryTerm)) {
                num = num + 1.0;
            }
        }

        double freq = 0;
        if (total != 0) {
            freq = num / total;
        }

        return freq;
    }

    public void connectSqlDB() throws Exception {
        // This will load the MySQL driver, each DB has its own driver
        String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        String DB_URL = "jdbc:mysql://172.29.27.171:3306/";
        String USER = "readpeer";
        String PASS = "readpeer";
        Class.forName(JDBC_DRIVER);

        connect = DriverManager.getConnection(DB_URL, USER, PASS);
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
                              String question, String result, double score, long start, long end, double globalScore, double localScore) {
        try {
            PreparedStatement preparedStatement = connect.prepareStatement("insert into tweets_db.quorasfull value (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            preparedStatement.setString(1, fid);
            preparedStatement.setLong(2, nodeid);
            preparedStatement.setString(3, bid);
            preparedStatement.setLong(4, pid);
            preparedStatement.setString(5, url);
            preparedStatement.setString(6, question);
            preparedStatement.setString(7, result);
            preparedStatement.setDouble(8, score);
            preparedStatement.setLong(9, start);
            preparedStatement.setLong(10, end);
            preparedStatement.setDouble(11, globalScore);
            preparedStatement.setDouble(12, localScore);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void writeDataBase(String tid, String text, double score, String bid, long pid, long start, long end) {
        try {
            PreparedStatement preparedStatement = connect.prepareStatement("insert into tweets_db.tweetsfull value " +
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

            PreparedStatement readStatement = connect.prepareStatement("select * from tweets_db.tweets where tid = ?");
            readStatement.setString(1, tid);
            ResultSet readSet = readStatement.executeQuery();

            readSet.next();
            java.sql.Timestamp stamp = readSet.getTimestamp("createtime");
            java.sql.Date createTime = new java.sql.Date(stamp.getTime());
            Long uid = readSet.getLong("uid");
            String screenName = readSet.getString("screenname");
            String name = readSet.getString("name");
            String profileUrl = readSet.getString("profileurl");
            java.sql.Date timeStamp = readSet.getDate("timestamp");
            int jobId = readSet.getInt("job_id");
            java.util.Date date = new Date();
            java.sql.Date sqlDate = new java.sql.Date(date.getTime());

            preparedStatement.setString(1, tid);
            preparedStatement.setDate(2, createTime);
            preparedStatement.setString(3, text);
            preparedStatement.setDouble(4, 0.0);
            preparedStatement.setDouble(5, 0.0);
            preparedStatement.setLong(6, uid);
            preparedStatement.setString(7, screenName);
            preparedStatement.setString(8, name);
            preparedStatement.setString(9, profileUrl);
            preparedStatement.setDate(10, timeStamp);
            preparedStatement.setInt(11, jobId);
            preparedStatement.setDouble(12, score);
            preparedStatement.setString(13, bid);
            preparedStatement.setLong(14, pid);
            preparedStatement.setInt(15, 7);
            preparedStatement.setInt(16, 0);
            preparedStatement.setString(17, "random");
            preparedStatement.setString(18, "random");
            preparedStatement.setInt(19, 0);
            preparedStatement.setDate(20, null);
            preparedStatement.setInt(21, 0);
            preparedStatement.setInt(22, 0);
            preparedStatement.setLong(23, start);
            preparedStatement.setLong(24, end);
            preparedStatement.setDate(25, sqlDate);
            preparedStatement.setInt(26, 0);
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
