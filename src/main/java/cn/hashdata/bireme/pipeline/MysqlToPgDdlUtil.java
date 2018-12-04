package cn.hashdata.bireme.pipeline;

import cn.hashdata.bireme.BiremeException;
import cn.hashdata.bireme.Row;
import cn.hashdata.bireme.TableAlertTypeEnum;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author: : yangyang.li
 * Created time: 2018/11/30.
 * Copyright(c) TTPai All Rights Reserved.
 * Description :
 */
public class MysqlToPgDdlUtil {


    private static Logger logger= LogManager.getLogger("Bireme." + MysqlToPgDdlUtil.class);

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
            resultSQL=createTableSql(record.def,sqlMysql);
        }
        if(Row.RowType.TABLE_ALTER == rowType){
            sqlMysql = sqlMysql.replaceAll("\r\n"," ").replaceAll("`","").replaceAll("/\\*.*\\*/","");
            try {
                resultSQL=tableAlterHandle(record.old,record.def,sqlMysql);
            } catch (Exception e) {
                logger.error("Row.RowType.TABLE_ALTER 类型失败:sql-{}",sqlMysql,e);
            }
        }
        return resultSQL;
    }

    /**
     * 表修改
     * 1.新增字段
     * 2.修改字段
     * 3.删除字段
     *@author: yangyang.li@ttpai.cn
     * @param def
     * @param sql
     *@return
     */
    private static String tableAlterHandle(JsonObject old,JsonObject def,String sql) throws Exception{
        Integer isPattern=  TableAlertTypeEnum.isPattern(sql);
        if(isPattern!=null && isPattern == 0){
            return null;
        }
        String database=def.get("database").getAsString();
        String oldTable=old.get("table").getAsString();
        String newTable=def.get("table").getAsString();
        StringBuilder sqlStr=new StringBuilder();
       switch (isPattern){
           case 1://table rename
                sqlStr.append(" alter table ").append(database).append(".").append("\"").append(oldTable).append("\"").append(" rename to ")
                .append("\"").append(newTable).append("\"");
               break;
           case 2://drop column
               JsonArray jsonArr= def.get("columns").getAsJsonArray();
               int arrSize= jsonArr.size();
               if(arrSize > 0){
                   sqlStr.append(" ALTER TABLE ").append(database).append(".").append("\"").append(newTable).append("\"");
                   for(int i=0;i<arrSize;i++){
                       JsonObject current= jsonArr.get(i).getAsJsonObject();
                       String columnName=current.get("name").getAsString();
                       sqlStr.append(" DROP COLUMN ").append(columnName);
                       if((i+1) != arrSize){
                           sqlStr.append(",");
                       }
                   }
               }
               break;
           case 3://table change
               break;
           case 4://add column
                int oldColumnSize= old.getAsJsonArray("columns").size();
                JsonArray newColumnArr= def.getAsJsonArray("columns");
                int newColumnSize=newColumnArr.size();
                if(newColumnSize > oldColumnSize){
                    sqlStr.append("ALTER TABLE ").append(database).append(".").append("\"").append(newTable).append("\"").append(" ");
                    for(int i=oldColumnSize; i< newColumnSize;i++){
                        JsonObject newColumn= newColumnArr.get(i).getAsJsonObject();
                        String newColumnName=newColumn.get("name").getAsString();
                        String newTypeName=newColumn.get("type").getAsString();
                        String pgType= typeLengthFromMysqlToPlum(newTypeName,newColumnName,sql);
                        sqlStr.append("ADD COLUMN ").append(newColumnName).append(pgType);
                        if((i+1) != newColumnSize ){
                            sqlStr.append(",");
                        }
                    }
                }
               break;
           case 5://modify column
               JsonArray oldArrayColumns=old.getAsJsonArray("columns");
               JsonArray newArrayColumns=def.getAsJsonArray("columns");
               if(oldArrayColumns.size() == newArrayColumns.size()){
                    JsonArray afterArrayType=new JsonArray();
                    JsonArray afterArrayColumn=new JsonArray();
                    JsonObject afterObjType=null;
                    JsonObject afterObjColumn=null;
                    for(int i=0;i < newArrayColumns.size() ; i++ ){
                        JsonObject newCurr=newArrayColumns.get(i).getAsJsonObject();
                        JsonObject oldCurr=oldArrayColumns.get(i).getAsJsonObject();
                        afterObjType=new JsonObject();
                        afterObjColumn = new JsonObject();
                        //列名相同 且 类型不同。修改类型
                        if(oldCurr.get("name").getAsString().equals(newCurr.get("name").getAsString()) &&
                                !oldCurr.get("type").getAsString().equals(newCurr.get("type").getAsString())    ){
                            afterObjType.addProperty("name",newCurr.get("name").getAsString());
                            afterObjType.addProperty("type",newCurr.get("type").getAsString());
                            afterArrayType.add(afterObjType);
                        }
                        // 列名不同。类型相同
                        if(!oldCurr.get("name").getAsString().equals(newCurr.get("name").getAsString()) &&
                                oldCurr.get("type").getAsString().equals(newCurr.get("type").getAsString())  ){
                            afterObjColumn.addProperty("oldName",oldCurr.get("name").getAsString());
                            afterObjColumn.addProperty("newName",newCurr.get("name").getAsString());
                            afterArrayColumn.add(afterObjColumn);
                        }
                    }
                   if(afterArrayColumn.size() > 0){
                       //修改列名字
                       int totalColumn= afterArrayColumn.size();
                       for(int i=0;i<totalColumn ;i++ ){
                           JsonObject columnCurr= afterArrayColumn.get(i).getAsJsonObject();
                           StringBuilder columnString=new StringBuilder();
                           columnString.append("ALTER TABLE ").append(database).append("\"").append(newTable).append("\"").append(" RENAME ");
                           columnString.append(columnCurr.get("oldName").getAsString()).append(" TO ").append(columnCurr.get("newName").getAsString()).append(";");
                           sqlStr.append(columnString.toString());
                       }
                   }
                    if(afterArrayType.size() > 0 ){
                        //修改列类型
                        int totalType= afterArrayType.size();
                        StringBuilder typeBuilder=new StringBuilder();
                        typeBuilder.append("ALTER TABLE ").append(database).append(".").append("\"").append(newTable).append("\"");
                        for(int i=0;i<totalType ;i++ ){
                            JsonObject typeCurr= afterArrayType.get(i).getAsJsonObject();
                            String columnName=  typeCurr.get("name").getAsString();
                            String pgType=typeLengthFromMysqlToPlum(typeCurr.get("type").getAsString(),columnName,sql);
                            typeBuilder.append(" ALTER COLUMN ").append(columnName).append(" TYPE ").append(pgType);
                            if((i+1)!=totalType){
                                typeBuilder.append(",");
                            }
                        }
                        sqlStr.append(typeBuilder.toString());
                    }
               }
               break;
       }
        return sqlStr.toString();
    }




    /**
     *  创建表。转换
     *@author: yangyang.li@ttpai.cn
     * @param columns
     * @param sql
     *@return
     */
    private static String createTableSql(JsonObject columns,String sql){
        String createSqlStr= null;
        try {
            if(columns== null || !columns.has("columns") || StringUtils.isBlank(sql)){
                return null;
            }
            JsonArray columnsArray= columns.get("columns").getAsJsonArray();
            if(columnsArray.isJsonNull() || columnsArray.size() <=0 ){
                return null;
            }
            String database=columns.get("database").getAsString();
            String table=columns.get("table").getAsString();
            StringBuilder createSql=new StringBuilder("CREATE TABLE ");
            createSql.append(database).append(".").append("\"").append(table).append("\"").append("(");
            for(int i=0;i<columnsArray.size();i++){
                JsonObject current=columnsArray.get(i).getAsJsonObject();
                String type=current.get("type").getAsString();
                String columnName=current.get("name").getAsString();
                String pgColumn=typeLengthFromMysqlToPlum(type,columnName,sql);
                createSql.append(" ").append(pgColumn).append(" ").append(",");
            }
            createSqlStr = createSql.toString();
            String primaryKey=null;
            if(columns.has("primary-key") && !columns.get("primary-key").isJsonNull()){
                primaryKey= columns.get("primary-key").getAsJsonArray().toString();
                primaryKey= primaryKey.substring(1,primaryKey.length()-1).replaceAll("\"","");
            }
            createSqlStr = createSqlStr + " PRIMARY KEY ("+primaryKey+")" + ")" +" DISTRIBUTED BY ("+primaryKey+") ; ";
        } catch (Exception e) {
            logger.error("构建postgreSQL 语句异常：",e);
        }
        return createSqlStr;
    }


    /**
     * 
     *@author: yangyang.li@ttpai.cn
     * @param type  mysql 列类型转 greenplum 列类型
     *@return
     */
    private static String typeLengthFromMysqlToPlum(String type,String columnName,String sql) throws Exception{
        String pgColumn=null;
        Boolean hasLength=true;
        switch (type.toUpperCase()){
            case "MEDIUMINT":
            case "INT":
            case "INTEGER":
            case "TINYINT":
            case "SMALLINT":
            case "BIGINT":
                pgColumn="bigint";
                break;
            case "FLOAT":
            case "DOUBLE":
            case "DECIMAL":
                pgColumn="numeric";
                break;
            case "DATE":
            case "TIME":
                hasLength = false;
                pgColumn=type;
                break;
            case "DATETIME":
            case "TIMESTAMP":
                hasLength = false;
                pgColumn="timestamp without time zone";
                break;
            case "CHAR":
            case "VARCHAR":
                pgColumn="varchar";
                break;
            case "BLOB":
            case "TEXT":
            case "MEDIUMBLOB":
            case "MEDIUMTEXT":
            case "LONGBLOB":
            case "LONGTEXT":
            case "TINYBLOB":
            case "TINYTEXT":
                hasLength = false;
                pgColumn = "text";
                break;
            case "ENUM":
            case "SET":
                hasLength = false;
                pgColumn="enum";
                break;
                default:
                    pgColumn = type;
        }
        String pgType=  replaceColumnType(columnName,pgColumn,sql,hasLength);
        return pgType;
    }


    /**
     *  解析sql中字段的类型
     *@author: yangyang.li@ttpai.cn
     * @param columnName
     * @param mysqlStr
     *@return
     */
    private static String replaceColumnType(String columnName,String pgType,String mysqlStr,Boolean hasLength) throws Exception{
        String subStr=  mysqlStr.substring(mysqlStr.indexOf(columnName)+columnName.length(),mysqlStr.length());
        if(subStr.startsWith("`") || subStr.startsWith(" ")){
            subStr = subStr.substring(1,subStr.length()).trim();
        }
        String columnTypeStr= subStr.substring(0,subStr.indexOf(",") == -1 ? subStr.length() : subStr.indexOf(","));
        if(columnTypeStr.indexOf(" ") > 0){
            columnTypeStr = columnTypeStr.substring(0,columnTypeStr.indexOf(" ") == -1 ? columnTypeStr.length() : columnTypeStr.indexOf(" "));
        }
        String trimStr=columnTypeStr!=null ? columnTypeStr.trim() : "";
        if(StringUtils.isBlank(trimStr)){
            return null;
        }
        String numType= trimStr.indexOf("(") > 0 && trimStr.indexOf(")") > 0 ? trimStr.substring(trimStr.indexOf("("),trimStr.indexOf(")")+1) : "";
        if(hasLength){
            return pgType+numType;
        }
        return pgType;
    }

    public static void main(String[] args) {

//        String sql="CREATE TABLE `BOSS_VIOLATE_BAK` (\r\n `ID` int(11) NOT NULL AUTO_INCREMENT,\r\n `VIOLATE_REASON` varchar(60) DEFAULT NULL COMMENT '违约原因',\r\n `VIOLATE_TYPE` int(11) DEFAULT NULL COMMENT '违约类型',\r\n `VIOLATE_TYPE_NAME` varchar(20) DEFAULT NULL COMMENT '违约类型描述',\r\n PRIMARY KEY (`ID`)\r\n) ENGINE=InnoDB AUTO_INCREMENT=88 DEFAULT CHARSET=utf8 COMMENT='违约原因类型'";
//        sql = sql.replaceAll("\r\n"," ").replaceAll("`","").replaceAll("/\\*.*\\*/","");
//        System.out.println(sql);
//        System.out.println(replaceColumnType("VIOLATE_REASON","varchare",sql,true));

        String ss="{\"primary-key\":[\"ID\",\"NAME\"]}";

        JsonParser jsonParser = new JsonParser();
        JsonObject value = jsonParser.parse(ss).getAsJsonObject();

        String arr= value.get("primary-key").getAsJsonArray().toString();
        String sss= arr.substring(1,arr.length()-1).replaceAll("\"","");
        System.out.println(sss);
    }

}
