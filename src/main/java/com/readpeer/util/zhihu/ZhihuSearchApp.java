package com.readpeer.util.zhihu;

import java.sql.ResultSet;

/**
 * Created by Beibei on 25/4/2016.
 */
public class ZhihuSearchApp {
    public static void main(String[] args) throws Exception {
//        try {
//            Indexer id = new Indexer("/Users/Beibei/Downloads/zhihu-index/");
//            id.createIndex();
//            id.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        ZhihuSearchClient msa = new ZhihuSearchClient("/Users/Beibei/Downloads/zhihu-index/");
        msa.connectSqlDB();
        ResultSet resultSet = msa.readDataBase("861265DEA6D17F96F976455BE8B79C8F");
        boolean localDupRemover = true;
        msa.searchAndUpdate(resultSet, localDupRemover, "termQuery");
        msa.close(resultSet);
    }
}
