package com.sesame.searcher;

/**
 * Created by sesame on 3/3/16.
 */
import apple.laf.JRSUIConstants;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.MultiFieldQueryParser;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by sesame on 19/2/16.
 */
public class Searcher {

    IndexSearcher indexSearcher;

    public Searcher(String indexDirectoryPath) throws IOException{
        Directory indexDirectory = FSDirectory.open(new File(indexDirectoryPath));
        indexSearcher = new IndexSearcher(indexDirectory);
    }

    public TopDocs search(List<String> searchQuerys) throws IOException, ParseException{
        BooleanQuery booleanQuery = new BooleanQuery();

        for(String searchQuery : searchQuerys) {
            TermQuery tq1 = new TermQuery(new Term(LuceneConstants.CONTENTS, searchQuery));
            TermQuery tq2 = new TermQuery(new Term(LuceneConstants.QUESTION, searchQuery));
            TermQuery tq3 = new TermQuery(new Term(LuceneConstants.CATEGORY, searchQuery));
            Float weight = 1.0f;
            tq1.setBoost(weight);
            tq2.setBoost(weight);
            tq3.setBoost(weight);
            booleanQuery.add(tq1, BooleanClause.Occur.SHOULD);
            booleanQuery.add(tq2, BooleanClause.Occur.SHOULD);
            booleanQuery.add(tq3, BooleanClause.Occur.SHOULD);

        }

        System.out.println(booleanQuery.toString());

        TopDocs td = indexSearcher.search(booleanQuery, LuceneConstants.MAX_SEARCH);
        return td;
    }

    public Document getDocument(ScoreDoc scoreDoc) throws IOException {
        return indexSearcher.doc(scoreDoc.doc);
    }

    public void close() throws IOException {
        indexSearcher.close();
    }

}
