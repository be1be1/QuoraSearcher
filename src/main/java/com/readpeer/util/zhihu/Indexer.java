package com.readpeer.util.zhihu;

import com.mongodb.*;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by Beibei on 25/4/2016.
 */
public class Indexer {

    private IndexWriter writer;
    private Mongo mongo;
    private DB db;
    private DBCollection col;
    private DBCursor cursor;

    public Indexer(String indexDirectoryPath) throws IOException {

        Directory indexDirectory = NIOFSDirectory.open(new File(indexDirectoryPath));
        SmartChineseAnalyzer analyzer = new SmartChineseAnalyzer(Version.LUCENE_45);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_45, analyzer);

        LMJelinekMercerSimilarity similarity = new LMJelinekMercerSimilarity(0.01f);
        config.setSimilarity(similarity);

        //Create the indexer using StandardAnalyser
        writer = new IndexWriter(indexDirectory, config);

        //Connect Mongodb
        mongo = new MongoClient();
        db = mongo.getDB("qadb");
        col = db.getCollection("zhihu");
        cursor = col.find();
    }

    public int createIndex() throws IOException{
        while(cursor.hasNext()) {
            DBObject obj = cursor.next();
            Document doc = new Document();

            //Add answer field
            StringBuilder answerStr = new StringBuilder();
            BasicDBObject ansObj = (BasicDBObject) obj.get("answer");
            int ansLen = ansObj.size();
            for (int j = 1; j <= ansLen ; j++) {
                String answer = ansObj.get("answer" + j).toString();
                answerStr.append(answer).append(" ");
            }
            TextField fieldAnswer = new TextField(com.readpeer.util.zhihu.LuceneConstants.CONTENTS, answerStr.toString(), Field.Store.YES);
            fieldAnswer.setBoost(4.0f);
            doc.add(fieldAnswer);

            //Add question field
            String question = obj.get("question").toString();
            TextField fieldQuestion= new TextField(com.readpeer.util.zhihu.LuceneConstants.QUESTION, question, Field.Store.YES);
            doc.add(fieldQuestion);

            //Add name field
            String name = obj.get("_id").toString();
            StringField fieldName = new StringField(com.readpeer.util.zhihu.LuceneConstants.FILE_NAME, name, Field.Store.YES);
            doc.add(fieldName);

            //Add category field
            List<String> categories = (List<String>) obj.get("categories");
            StringBuilder categoryStr = new StringBuilder();
            for (String category : categories) {
                category.replace("\"", "");
                categoryStr.append(category).append(" ");
            }
            TextField fieldCategory = new TextField(com.readpeer.util.zhihu.LuceneConstants.CATEGORY, categoryStr.toString(), Field.Store.YES);
            doc.add(fieldCategory);

            String url = obj.get("url").toString();
            StoredField fieldUrl = new StoredField(com.readpeer.util.zhihu.LuceneConstants.URL, url);
            doc.add(fieldUrl);

            writer.addDocument(doc);
        }

        return writer.numDocs();
    }

    public void close() throws IOException {
        //Close Indexer
        writer.close();
    }

}
