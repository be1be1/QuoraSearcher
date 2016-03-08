package com.sesame.searcher;

/**
 * Created by sesame on 3/3/16.
*/
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by sesame on 19/2/16.
 */
public class Searcher {

    IndexSearcher indexSearcher;
    Directory indexDirectory = null;
    IndexReader indexReader = null;

    public Searcher(String indexDirectoryPath) throws IOException{
        indexDirectory = FSDirectory.open(new File(indexDirectoryPath));
        indexReader = DirectoryReader.open(indexDirectory);
        indexSearcher = new IndexSearcher(indexReader);
    }

    public TopDocs search(List<String> searchQuerys) throws IOException{
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

        TopDocs td = indexSearcher.search(booleanQuery, LuceneConstants.MAX_SEARCH);
        return td;
    }

    public Document getDocument(ScoreDoc scoreDoc) throws IOException {
        return indexSearcher.doc(scoreDoc.doc);
    }

    public void close() throws IOException {
        indexReader.close();
    }

}
