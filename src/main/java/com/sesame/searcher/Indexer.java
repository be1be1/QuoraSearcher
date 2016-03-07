package com.sesame.searcher;

import com.mongodb.*;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;

import java.util.List;

/**
 * Created by sesame on 18/2/16.
 */
public class Indexer {

    private IndexWriter writer;
    private Mongo mongo;
    private DB db;
    private DBCollection col;
    private DBCursor cursor;

    public Indexer(String indexDirectoryPath) throws IOException {
        //Directory that contains indexes
        Directory indexDirectory = FSDirectory.open(new File(indexDirectoryPath));

        //Create the indexer using StandardAnalyser
        writer = new IndexWriter(indexDirectory, new StandardAnalyzer(Version.LUCENE_36), true, IndexWriter.MaxFieldLength.UNLIMITED);

        //Connect Mongodb
        mongo = new MongoClient();
        db = mongo.getDB("qadb");
        col = db.getCollection("quora");
        cursor = col.find();
    }

    public int createIndex() throws IOException{
        for(int i = 0; i < 500000; i++) {
            if (cursor.hasNext()) {
                DBObject obj = cursor.next();
                Document doc = new Document();

                //Add answer field
                StringBuilder answerStr = new StringBuilder();
                BasicDBObject ansObj = (BasicDBObject) obj.get("answer");
                int ansLen = ansObj.size();
                for (int j = 1; j < ansLen - 1; j++) {
                    String answer = ansObj.get("answer" + j).toString();
                    answerStr.append(answer).append(" ");
                }
                Field fieldAnswer = new Field(LuceneConstants.CONTENTS, answerStr.toString(), Field.Store.YES, Field.Index.ANALYZED);
                fieldAnswer.setBoost(4.0f);
                doc.add(fieldAnswer);

                //Add question field
                String question = obj.get("question").toString();
                Field fieldQuestion = new Field(LuceneConstants.QUESTION, question, Field.Store.YES, Field.Index.ANALYZED);
                fieldQuestion.setBoost(3.0f);
                doc.add(fieldQuestion);

                //Add name field
                String name = obj.get("_id").toString();
                Field fieldName = new Field(LuceneConstants.FILE_NAME, name, Field.Store.YES, Field.Index.NOT_ANALYZED);
                doc.add(fieldName);

                //Add category field
                List<String> categories = (List<String>) obj.get("categories");
                StringBuilder categoryStr = new StringBuilder();
                for (String category : categories) {
                    category.replace("\"", "");
                    categoryStr.append(category).append(" ");
                }
                Field fieldCategory = new Field(LuceneConstants.CATEGORY, categoryStr.toString(), Field.Store.YES, Field.Index.ANALYZED);
                fieldCategory.setBoost(3.0f);
                doc.add(fieldCategory);

                String url = obj.get("url").toString();
                Field fieldUrl = new Field(LuceneConstants.URL, name, Field.Store.YES, Field.Index.NOT_ANALYZED);
                doc.add(fieldUrl);

                writer.addDocument(doc);
            }
        }

        return writer.numDocs();
    }

    public void close() throws CorruptIndexException, IOException {
        //Close Indexer
        writer.close();
    }

}
