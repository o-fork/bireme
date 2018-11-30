package cn.hashdata.bireme.pipeline;

import cn.hashdata.bireme.Row;
import org.apache.commons.lang3.StringUtils;

/**
 * @author: : yangyang.li
 * Created time: 2018/11/30.
 * Copyright(c) TTPai All Rights Reserved.
 * Description :
 */
public class MysqlToPgDdlUtil {



    /**
     * mysql ddl语句转 greenplum ddl语句
     *@author: yangyang.li@ttpai.cn
     * @param rowType
     * @param record
     *@return
     */
    public static String tableAlter(Row.RowType rowType,MaxwellPipeLine.MaxwellTransformer.MaxwellRecord record){
        String sqlMysql=record.sql;
        if(StringUtils.isBlank(sqlMysql)){
            return "";
        }
        String resultSQL="";
        if(Row.RowType.DATABASE_DROP == rowType){
            String dropDb="DROP DATABASE ";
            resultSQL= dropDb + record.database;
        }
        if(Row.RowType.DATABASE_CREATE == rowType){
            String createBd="CREATE DATABASE ";
            resultSQL= createBd + record.database;
        }
        if(Row.RowType.TABLE_DROP == rowType){
            resultSQL="DROP TABLE "+record.database+".\""+record.table+"\"";
        }
        if(Row.RowType.TABLE_CREATE == rowType){
            sqlMysql = sqlMysql.replaceAll("\r\n"," ").replaceAll("`","").replaceAll("/\\*.*\\*/","");






        }
        return resultSQL;
    }


}
