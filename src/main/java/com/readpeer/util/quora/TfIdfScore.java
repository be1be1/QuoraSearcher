package com.readpeer.util.quora;

import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;

/**
 * Created by sesame on 24/3/16.
 */
public class TfIdfScore {

    Directory indexDirectory = null;
    IndexReader indexReader = null;


    public TfIdfScore(String indexDirectoryPath) throws IOException{

        indexDirectory = FSDirectory.open(new File(indexDirectoryPath));
        indexReader = DirectoryReader.open(indexDirectory);

    }

    public double documentFrequency(String word) throws IOException{
        word = word.toLowerCase();
        System.out.println(word);
        Term t = new Term(LuceneConstants.CONTENTS, word);
        double totalDoc = 0;
        double idf = 0;

        double docFreq = indexReader.docFreq(t);
        if(docFreq != 0) {
            totalDoc = (double) indexReader.numDocs();
            idf = Math.log(totalDoc / docFreq);
            System.out.println("docFreq is: " + docFreq);
        } else {
            idf = 0;
        }

        return idf;
    }

    public double getIdfScore(String phrase) throws IOException{
        String[] words = phrase.split(" ");
        double total = 0.0;
        for(String word : words) {
            total += documentFrequency(word);
        }

        System.out.println(words.length);
        double result = total/words.length;
        return result;
    }
}
