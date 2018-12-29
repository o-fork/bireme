package cn.hashdata.bireme.pipeline;

import cn.hashdata.bireme.BiremeException;
import cn.hashdata.bireme.BiremeUtility;
import cn.hashdata.bireme.Context;
import cn.hashdata.bireme.GetPrimaryKeys;
import cn.hashdata.bireme.Row;
import cn.hashdata.bireme.Table;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropColumnItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableOption;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import com.alibaba.druid.util.JdbcConstants;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author: : yangyang.li
 * Created time: 2018/11/30.
 * Copyright(c) TTPai All Rights Reserved.
 * Description :
 */
public class MysqlToPgDdlUtil {


    private static Logger logger= LogManager.getLogger("Bireme." + MysqlToPgDdlUtil.class);
    private static List<String> intType=new ArrayList<>();
    private static List<String> charType=new ArrayList<>();
    private static List<String> bigType=new ArrayList<>();
    private static List<String> dateType=new ArrayList<>();
    private static List<String> floatType=new ArrayList<>();
    private static List<String> timeType=new ArrayList<>();
    static {
        intType.add("tinyint");
        intType.add("smallint");
        intType.add("mediumint");
        intType.add("int");
        intType.add("bigint");
        floatType.add("float");
        floatType.add("double");
        floatType.add("decimal");
        charType.add("char");
        charType.add("varchar");
        bigType.add("tinyblob");
        bigType.add("tinytext");
        bigType.add("blob");
        bigType.add("text");
        bigType.add("mediumblob");
        bigType.add("mediumtext");
        bigType.add("longblob");
        bigType.add("longtext");
        dateType.add("date");
        dateType.add("timestamp");
        dateType.add("datetime");
        timeType.add("time");
        timeType.add("year");
        timeType.add("enum");
        timeType.add("set");
    }


    public static String tableAlter(Row.RowType rowType,MaxwellPipeLine.MaxwellTransformer.MaxwellRecord record,Row row){
        String sqlMysql=record.sql;
        if(StringUtils.isBlank(sqlMysql)){
            return "";
        }
        sqlMysql = sqlMysql.replaceAll("\r\n"," ").replaceAll("`","").replaceAll("/\\*.*\\*/","");
        List<SQLStatement> statementList= SQLUtils.parseStatements(sqlMysql, JdbcConstants.MYSQL);
        if(statementList == null){
            return "";
        }
        String resultSQL="";
        if(Row.RowType.TABLE_CREATE == rowType){
            resultSQL=createTableSql(record.def,statementList);
        }
        if(Row.RowType.DATABASE_DROP == rowType){
            String dropDb="DROP DATABASE ";
            resultSQL= dropDb + record.database;
        }
        if(Row.RowType.DATABASE_CREATE == rowType){
            String createBd="CREATE DATABASE ";
            resultSQL= createBd + record.database;
        }
        if(Row.RowType.TABLE_DROP == rowType){
            resultSQL="DROP TABLE "+record.database+".\""+record.table.toUpperCase()+"\"";
        }
        if(Row.RowType.TABLE_ALTER == rowType){
            try {
                resultSQL=tableAlterHandle(record.old,record.def,statementList,row);
            } catch (Exception e) {
                logger.error("Row.RowType.TABLE_ALTER 类型失败:sql-{}",sqlMysql,e);
            }
        }
        return resultSQL;
    }


    private static String tableAlterHandle(JsonObject old,JsonObject def,List<SQLStatement> statementList,Row row) throws Exception{
        StringBuilder alterSQL=new StringBuilder();
        String database=def.get("database").getAsString();
        String oldTable=old.get("table").getAsString();
        String newTable=def.get("table").getAsString();
        for(SQLStatement statement:statementList){
            //rename table
            if(statement instanceof MySqlRenameTableStatement){
                MySqlRenameTableStatement renameTableStatement=(MySqlRenameTableStatement)statement;
                List<MySqlRenameTableStatement.Item> listItems=renameTableStatement.getItems();
                for(MySqlRenameTableStatement.Item item:listItems){
                    StringBuilder renameSQL=new StringBuilder();
                    String beforeName= item.getName().getSimpleName();
                    String afterName= item.getTo().getSimpleName();
                    renameSQL.append("alter table ").append(database).append(".\"").append(beforeName.toUpperCase()).append("\"").append(" ")
                            .append(" rename to ").append("\"").append(afterName).append("\"").append(";");
                    alterSQL.append(renameSQL.toString());
                }
                row.renameTable = true;
                return alterSQL.toString();
            }
            if(statement instanceof SQLAlterTableStatement){
                SQLAlterTableStatement alterTableStatement=(SQLAlterTableStatement)statement;
                List<SQLAlterTableItem> listAlter=alterTableStatement.getItems();
                List<String> addColumnList=new ArrayList<>();
                List<String> changeColumnList=new ArrayList<>();
                List<String> alterTableColumnComment=new ArrayList<>();
                String addColumnSQLStr=null;
                String dropColumnSQLStr=null;
                String modifyColumnSQLStr=null;
                StringBuilder  addColumnSQL=new StringBuilder();
                addColumnSQL.append("ALTER TABLE ").append(database).append(".\"").append(newTable).append("\"").append(" ");
                StringBuilder dropColumnSQL=new StringBuilder();
                dropColumnSQL.append("ALTER TABLE ").append(database).append(".\"").append(newTable).append("\" ");
                StringBuilder modifyColumnSQL=new StringBuilder();
                modifyColumnSQL.append("ALTER TABLE ").append(database).append(".\"").append(newTable).append("\"").append(" ");
                boolean hasAddColumn=false;
                boolean hasDropColumn=false;
                boolean hasModifyColumn=false;
                for(SQLAlterTableItem item:listAlter){
                    //保存column
                    if(item instanceof SQLAlterTableAddColumn){
                        hasAddColumn = true;
                        SQLAlterTableAddColumn addColumn=(SQLAlterTableAddColumn)item;
                        List<SQLColumnDefinition> listColumns= addColumn.getColumns();
                        for(SQLColumnDefinition columnDefinition:listColumns){
                            String columnName= columnDefinition.getName().getSimpleName();
                            List<SQLExpr> listExpr=columnDefinition.getDataType().getArguments();
                            String columnType=mysqlTypeToPgType(columnDefinition.getDataType().getName(),listExpr);
                            addColumnSQL.append(" ADD COLUMN ").append(columnName).append(" ").append(columnType).append(",");
                            //----------备注-------------------------
                            if(columnDefinition.getComment() instanceof SQLCharExpr){
                                SQLCharExpr charExpr=(SQLCharExpr)columnDefinition.getComment();
                                StringBuilder commentSQL=new StringBuilder();
                                String comment=charExpr.getText()==null ? "" :charExpr.getText() ;
                                comment=comment.replaceAll("\"","").replaceAll("'","");
                                commentSQL.append("COMMENT ON COLUMN ").append("\"").append(database).append("\"").append(".")
                                        .append("\"").append(newTable).append("\"").append(".").append("").append(columnName).append("")
                                        .append(" IS ").append("'").append(comment).append("'").append(";");
                                addColumnList.add(commentSQL.toString());
                            }
                        }
                    }
                    //删除 column
                    if(item instanceof SQLAlterTableDropColumnItem){
                        hasDropColumn = true;
                        SQLAlterTableDropColumnItem dropColumnItem=(SQLAlterTableDropColumnItem)item;
                        List<SQLName> listSQLName= dropColumnItem.getColumns();
                        for(SQLName sqlName:listSQLName){
                            String removeColumn= sqlName.getSimpleName();
                            dropColumnSQL.append(" DROP COLUMN ").append(removeColumn).append(",");
                        }
                    }
                    //modify column
                    if(item instanceof MySqlAlterTableModifyColumn){
                        hasModifyColumn = true;
                        MySqlAlterTableModifyColumn modifyColumn=(MySqlAlterTableModifyColumn)item;
                        SQLColumnDefinition columnDefinition=  modifyColumn.getNewColumnDefinition();
                        String columnName= columnDefinition.getName().getSimpleName();
                        String columnType= checkColumnType(old,columnDefinition.getDataType().getName(),columnName);
                        //修改列类型
                        modifyColumnSQL.append(" ALTER COLUMN ").append(columnName).append(" TYPE ").append(columnType).append(",");
                        //修改列注释
                        if(columnDefinition.getComment()!=null && columnDefinition.getComment() instanceof SQLCharExpr) {
                            SQLCharExpr commentExpr=(SQLCharExpr)columnDefinition.getComment();
                            String comment=commentExpr.getText();
                            if(StringUtils.isNotBlank(comment)){
                                StringBuilder modifyColumnComment=new StringBuilder();
                                comment = comment.replaceAll("\"","").replaceAll("'","");
                                modifyColumnComment.append(" COMMENT ON COLUMN ").append("\"").append(database).append("\"").append(".")
                                        .append("\"").append(newTable).append("\"").append(".").append("").append(columnName).append("")
                                        .append(" IS ").append("'").append(comment).append("'").append(";");
                                alterTableColumnComment.add(modifyColumnComment.toString());
                            }
                        }
                    }
                    //修改列名字
                    if(item instanceof MySqlAlterTableChangeColumn){
                        MySqlAlterTableChangeColumn changeColumn=(MySqlAlterTableChangeColumn)item;
                        String oldColumnName= changeColumn.getColumnName().getSimpleName();
                        SQLColumnDefinition columnDefinitionChange= changeColumn.getNewColumnDefinition();
                        String newColumnName=columnDefinitionChange.getName().getSimpleName();
                        // 修改列名字
                        StringBuilder changeColumnSQL=new StringBuilder();
                        changeColumnSQL.append("ALTER TABLE ").append(database).append(".\"").append(newTable).append("\" RENAME ").append(oldColumnName)
                                .append(" TO ").append(newColumnName).append(";");
                        changeColumnList.add(changeColumnSQL.toString());
                        //修改类型
                        StringBuilder changeColumnTypeSQL=new StringBuilder();
                        if(columnDefinitionChange.getDataType()!=null && columnDefinitionChange.getDataType().getName()!=null){
                            String columnType= checkColumnType(old,columnDefinitionChange.getDataType().getName(),oldColumnName);
                            changeColumnTypeSQL.append("ALTER TABLE ").append(database).append(".\"").append(newTable).append("\" ").append("ALTER ")
                                    .append(" COLUMN ").append(newColumnName).append(" TYPE ").append(columnType).append(";");
                            changeColumnList.add(changeColumnTypeSQL.toString());
                        }
                        //修改列注释
                        if(columnDefinitionChange.getComment()!=null && columnDefinitionChange.getComment() instanceof SQLCharExpr) {
                            SQLCharExpr commentChange=(SQLCharExpr)columnDefinitionChange.getComment();
                            if(StringUtils.isNotBlank(commentChange.getText())){
                                StringBuilder modifyColumnComment=new StringBuilder();
                                String comment= commentChange.getText();
                                comment = comment.replaceAll("\"","").replaceAll("'","");
                                modifyColumnComment.append(" COMMENT ON COLUMN ").append("\"").append(database).append("\"").append(".")
                                        .append("\"").append(newTable).append("\"").append(".").append("").append(newColumnName).append("")
                                        .append(" IS ").append("'").append(comment).append("'").append(";");
                                alterTableColumnComment.add(modifyColumnComment.toString());
                            }
                        }
                    }
                    //修改表注释
                    if(item instanceof MySqlAlterTableOption){
                        MySqlAlterTableOption alterTableOption=(MySqlAlterTableOption)item;
                        if(alterTableOption.getValue()!=null && alterTableOption.getValue() instanceof SQLIdentifierExpr){
                            SQLIdentifierExpr  sqlIdentifierExpr=(SQLIdentifierExpr)alterTableOption.getValue();
                            StringBuilder alterTableComment=new StringBuilder();
                            String comment= sqlIdentifierExpr.getName();
                            comment = comment.replaceAll("\"","").replaceAll("'","");
                            alterTableComment.append("COMMENT ON TABLE \"").append(database).append("\".").append("\"").append(newTable).append("\"")
                                    .append(" IS '").append(comment).append("';");
                            alterTableColumnComment.add(alterTableComment.toString());
                        }
                    }
                }
                addColumnSQLStr=addColumnSQL.toString();
                if(StringUtils.isNotBlank(addColumnSQLStr) && addColumnSQLStr.endsWith(",")){
                    addColumnSQLStr = addColumnSQLStr.substring(0,addColumnSQLStr.length()-1);
                }
                dropColumnSQLStr=dropColumnSQL.toString();
                if(StringUtils.isNotBlank(dropColumnSQLStr) && dropColumnSQLStr.endsWith(",")){
                    dropColumnSQLStr = dropColumnSQLStr.substring(0,dropColumnSQLStr.length()-1);
                }
                if(StringUtils.isNotBlank(addColumnSQLStr) && hasAddColumn){
                    alterSQL.append(addColumnSQLStr).append(";");
                }
                modifyColumnSQLStr = modifyColumnSQL.toString();
                if(StringUtils.isNotBlank(modifyColumnSQLStr) && modifyColumnSQLStr.endsWith(",")){
                    modifyColumnSQLStr = modifyColumnSQLStr.substring(0,modifyColumnSQLStr.length()-1);
                }
                if(CollectionUtils.isNotEmpty(addColumnList)){
                    for(String comment:addColumnList){
                        alterSQL.append(comment);
                    }
                }
                if(StringUtils.isNotBlank(dropColumnSQLStr) && hasDropColumn){
                    alterSQL.append(dropColumnSQLStr).append(";");
                }
                if(StringUtils.isNotBlank(modifyColumnSQLStr) && hasModifyColumn){
                    alterSQL.append(modifyColumnSQLStr).append(";");
                }
                if(CollectionUtils.isNotEmpty(changeColumnList)){
                    for(String changeSql:changeColumnList){
                        alterSQL.append(changeSql);
                    }
                }
                if(CollectionUtils.isNotEmpty(alterTableColumnComment)){
                    for(String alterTableComment:alterTableColumnComment){
                        alterSQL.append(alterTableComment);
                    }
                }
                return alterSQL.toString();
            }
        }
        return null;
    }




    private static String checkColumnType(JsonObject old,String newType,String columnName){
        String oldType=null;
        if(old.has("columns") && !old.get("columns").isJsonNull()){
            JsonArray jsonArray= old.getAsJsonArray("columns");
            for(int i=0;i<jsonArray.size();i++){
                JsonObject curr=jsonArray.get(i).getAsJsonObject();
                if(curr.has("name") && columnName.equals(curr.get("name").getAsString())){
                    oldType= curr.get("type").getAsString();
                    String newModifyType= checkMysqlTypeToPgType(oldType,newType);
                    if(newModifyType!=null){
                        return newModifyType;
                    }
                }
            }
        }
        return oldType;
    }



    private static String checkMysqlTypeToPgType(String oldType,String newType){
        oldType = oldType.toLowerCase().trim();
        newType = newType.toLowerCase().trim();
        if(intType.contains(oldType) && intType.contains(newType)){
            return "bigint";
        }
        if(intType.contains(oldType) || timeType.contains(oldType)  ){
            if(floatType.contains(newType)){
                return "numeric(20,4)";
            }
            return "varchar(50)";
        }
        if(charType.contains(oldType) && charType.contains(newType)){
            return "varchar(200)";
        }
        if(dateType.contains(oldType) && dateType.contains(newType)){
            return "timestamp without time zone";
        }
        if(bigType.contains(newType)){
            return "text";
        }
        return "varchar(60)";
    }


    private static String createTableSql(JsonObject columns,List<SQLStatement> statementList){
        String createSqlStr= null;
        try {
            String database=columns.get("database").getAsString();
            String table=columns.get("table").getAsString();
            StringBuilder createSql=new StringBuilder("CREATE TABLE ");
            createSql.append("\"").append(database).append("\"").append(".").append("\"").append(table).append("\"").append("(");
            for(SQLStatement statement:statementList){
                if(statement instanceof MySqlCreateTableStatement){//创建表
                    List<String> listKey=new ArrayList<>();
                    MySqlCreateTableStatement createTableDDl=(MySqlCreateTableStatement)statement;
                    List<SQLTableElement> listColumns=  createTableDDl.getTableElementList();
                    List<String> listComment=new ArrayList<>();
                    for(SQLTableElement column:listColumns){
                        if(column instanceof SQLColumnDefinition){
                            SQLColumnDefinition currentItems=(SQLColumnDefinition)column;
                            String columnName=currentItems.getNameAsString();
                            List<SQLExpr> listExpr=currentItems.getDataType().getArguments();
                            String columnType=mysqlTypeToPgType(currentItems.getDataType().getName(),listExpr);
                            createSql.append("\"").append(columnName.toLowerCase()).append("\"").append(" ").append(columnType).append(",");

                            //----------备注----------------
                           if(currentItems.getComment() instanceof SQLCharExpr){
                               StringBuilder commentSb=new StringBuilder();
                               SQLCharExpr sqlCharExpr=(SQLCharExpr)currentItems.getComment();
                               String comment=sqlCharExpr.getText()==null ? "" : sqlCharExpr.getText();
                               comment = comment.replaceAll("\"","").replaceAll("'","");
                               commentSb.append("COMMENT ON COLUMN ").append("\"").append(database).append("\"").append(".")
                                       .append("\"").append(table).append("\"").append(".").append("").append(columnName).append("")
                                       .append(" IS ").append("'").append(comment).append("'").append(";");
                               listComment.add(commentSb.toString());
                           }
                        }
                        if(column instanceof MySqlPrimaryKey){
                            MySqlPrimaryKey primaryKey=(MySqlPrimaryKey)column;
                            List<SQLSelectOrderByItem> listPrimaryKey= primaryKey.getColumns();
                            for(SQLSelectOrderByItem item:listPrimaryKey){
                                if(item.getExpr() instanceof SQLIdentifierExpr){
                                    SQLIdentifierExpr identifierExpr=(SQLIdentifierExpr)item.getExpr();
                                    listKey.add("\""+identifierExpr.getName().toLowerCase()+"\"");
                                }
                            }
                        }
                    }
                    createSql.append(" CONSTRAINT ").append(table).append("_pkey").append(" PRIMARY KEY ").append("(")
                            .append(StringUtils.join(listKey,",")).append(")");
                    createSql.append(")");
                    createSql.append(";");
                    //---------------备注----------------------
                    if(CollectionUtils.isNotEmpty(listComment)){
                        for(String comment:listComment){
                            if(StringUtils.isNotBlank(comment)){
                                createSql.append(comment);
                            }
                        }
                    }
                    createSqlStr = createSql.toString();
                }
            }
        } catch (Exception e) {
            logger.error("构建postgreSQL 语句异常：",e);
        }
        return createSqlStr;
    }


  
    public static Table reflushTableAfterDDl(String fullTableName, Connection conn,Context cxt) throws BiremeException{
        HashMap<String,String> paramMap=new HashMap<>();
        paramMap.put(fullTableName,fullTableName);
        String[] tableName = fullTableName.split("\\.");
        Map<String, List<String>> listKeyMap= GetPrimaryKeys.getRefulshPrimaryKeys(paramMap,conn);
        Table table=new Table(tableName[0],tableName[1],listKeyMap,conn,null);
        cxt.tablesInfo.put(fullTableName,table);
        //更新 内存配置文件。此处写死，使用maxwell
        String fullName=fullTableName;
        fullName = fullName.replaceAll("\"","");
        cxt.tableMap.put(findConfigName(cxt)+"."+fullName,fullTableName);
        return table;
    }


    private static String findConfigName(Context cxt){
        String datasource="maxwell1";
        HashMap<String,String> tabMap= cxt.tableMap;
        Set<String> keySet= tabMap.keySet();
        String keyValue=null;
        for(String key:keySet){
           if(StringUtils.isNotBlank(key)){
               keyValue = key;
               break;
           }
        }
        if(StringUtils.isNotBlank(keyValue)){
           String[] keyArray= keyValue.split("\\.");
           datasource = keyArray[0];
        }
        return datasource;
    }


    private static String mysqlTypeToPgType(String dataType,List<SQLExpr> sqlExprList) throws Exception{
        String pgColumn="";
        String lengthStr=null;
        if(CollectionUtils.isNotEmpty(sqlExprList)){
            StringBuilder length=new StringBuilder("(");
            for(SQLExpr sqlExpr:sqlExprList){
                if( sqlExpr instanceof SQLIntegerExpr){
                    SQLIntegerExpr colum=(SQLIntegerExpr)sqlExpr;
                    if( colum != null && colum.getNumber()!=null && colum.getNumber().intValue() > 0 ){
                        int number= (colum.getNumber().intValue()*2 );
                        length.append( number >= 600 ? 600 : number);
                        if(sqlExprList.size() > 1){
                            length.append(",");
                        }
                    }
                }
            }
            lengthStr=length.toString();
            if(StringUtils.isNotBlank(lengthStr) && lengthStr.endsWith(",")){
                lengthStr = lengthStr.substring(0,lengthStr.length()-1);
            }
            lengthStr = lengthStr +")";
        }
        switch (dataType.toUpperCase()){
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
                pgColumn="numeric"+ (lengthStr!=null ? lengthStr : "");
                break;
            case "DATE":
            case "TIME":
                pgColumn=dataType;
                break;
            case "DATETIME":
            case "TIMESTAMP":
                pgColumn="timestamp without time zone";
                break;
            case "CHAR":
            case "VARCHAR":
                pgColumn="varchar"+ (lengthStr!=null ? lengthStr : "");
                break;
            case "BLOB":
            case "TEXT":
            case "MEDIUMBLOB":
            case "MEDIUMTEXT":
            case "LONGBLOB":
            case "LONGTEXT":
            case "TINYBLOB":
            case "TINYTEXT":
                pgColumn = "text";
                break;
            case "ENUM":
            case "SET":
                pgColumn="enum";
                break;
            default:
                pgColumn = dataType;
        }
        return pgColumn;
    }


    public static void handleDDlTableSql( Row row, Context cxt) throws BiremeException{
        Connection conn = BiremeUtility.jdbcConn(cxt.conf.targetDatabase);
        if(conn != null && StringUtils.isNotBlank(row.pgSql) && row.type != Row.RowType.TABLE_ALTER){
            logger.error("--------handleDDlTableSql---------pgSQL--------{}",row.pgSql);
            if(row.type == Row.RowType.TABLE_DROP){
                try {
                    executeDdlSql(conn,row.pgSql);
                } catch (BiremeException e) {
                    logger.error("---deleteTable--------异常",e);
                }
            }else{
                executeDdlSql(conn,row.pgSql);
            }

            try {
                //加入内存中
                if(row.type == Row.RowType.TABLE_CREATE){
                    logger.info("-----handleDDlTableSql----createTable-fullName---------:{}",row.tableFullName);
                    reflushTableAfterDDl(row.tableFullName,conn,cxt);
                    //更新配置文件 新增
                    reflushConfigProperties("",row.tableFullName,"maxwell1");
                }else if(row.type == Row.RowType.TABLE_DROP){
                    logger.info("----handleDDlTableSql-----dropTable-fullName---------:{}",row.tableFullName);
                    //更新配置文件 删除
                    reflushConfigProperties(row.tableFullName,"","maxwell1");
                }
            } catch (Exception e) {
               logger.info("------新建表-----加入内存中异常--------fullName:{}","",e);
            }
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error("--------createTableStart---------SQLException--------{}",row.pgSql,e);
            }
        }
    }


    public static Boolean executeDdlSql(Connection conn,String ddlSql) throws BiremeException {
        List<String> listDdl=  Arrays.asList(ddlSql.split(";"));
        if(CollectionUtils.isNotEmpty(listDdl)){
            Statement statement=null;
            try {
                statement= conn.createStatement();
                for(String ddl:listDdl){
                    if(StringUtils.isNotBlank(ddl)){
                        statement.execute(ddl);
                    }
                }
            } catch (Exception e) {
                logger.error("-----------execute--ddl---error---ddlSQL:{}",ddlSql,e);
                throw new BiremeException("-----------execute--ddl---error---ddlSQL------",e);
            }finally {
                if(statement != null){
                    try {
                        statement.close();
                    } catch (SQLException e) {
                        logger.error("-----------execute--ddl---error---ddlSQL------",e);
                    }
                }
            }
            return true;
        }
        return false;
    }


    /*
   *  需改表名，新增表名 更新磁盘中配置文件
   */
    public static synchronized void reflushConfigProperties(String oldTable,String newTable,String dataSource) throws BiremeException{
        try {
            Configurations configs = new Configurations();
            Configuration tableConfig = null;
            tableConfig = configs.properties(new File("etc"+ File.separator + dataSource + ".properties"));
            TreeMap<String,String> newHashMap=new TreeMap<>();
            if(StringUtils.isNotBlank(newTable)){
                newHashMap.put(newTable.replaceAll("\"",""),newTable);
            }
            if(tableConfig!=null){
                Iterator<String> tables = tableConfig.getKeys();
                while (tables.hasNext()) {
                    String mysqlTable = tables.next();
                    String pgTable = tableConfig.getString(mysqlTable);
                    if(StringUtils.isBlank(oldTable) || !mysqlTable.equals(oldTable.replaceAll("\"",""))){
                        newHashMap.put(mysqlTable,pgTable);
                    }
                }
            }
            StringBuilder stringBuilder=new StringBuilder();
            for(Map.Entry<String,String> currentMap:newHashMap.entrySet()){
                String key=currentMap.getKey();
                String value=currentMap.getValue();
                stringBuilder.append(key).append("=").append(value).append("\r\n");
            }
            FileWriter fileWriter=new FileWriter(new File("etc"+File.separator + dataSource + ".properties"));
            fileWriter.write(stringBuilder.toString());
            fileWriter.flush();
            fileWriter.close();
        } catch (Exception e) {
           logger.error("---更新磁盘配置文件失败-------oldTable:{}----newTable:{}-",oldTable,newTable,e);
           throw new BiremeException("---更新磁盘配置文件失败-------oldTable:{}----newTable:{}-",e);
        }
    }
}
