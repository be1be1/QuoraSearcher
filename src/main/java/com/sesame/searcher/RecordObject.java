package com.sesame.searcher;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by sesame on 4/3/16.
 */
public class RecordObject {
    private long jobId = 0;
    private String bookId = null;
    private String query = null;
    private long start = 0;
    private long end = 0;

    public RecordObject(long jobId, String bookId, String query, long start, long end) {
        this.jobId = jobId;
        this.bookId = bookId;
        this.query = query;
        this.start = start;
        this.end = end;
    }

    public long getJobId() {
        return jobId;
    }

    public long getStartTime() {
        return start;
    }

    public long getEndTime() { return end; }

    public String getBookId() {
        return bookId;
    }

    public String getQuery() {
        return query;
    }
}
