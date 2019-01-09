package cn.hashdata.bireme;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.util.JdbcConstants;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.daemon.DaemonInitException;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author: : yangyang.li
 * Created time: 2018/11/28.
 * Copyright(c) TTPai All Rights Reserved.
 * Description :
 */
public class Test {


    public static void main(String[] args) {

        String sqlMysql = "create table A_TEST(ID int not null auto_increment primary key,name varchar(20))";
        List<SQLStatement> statementList= SQLUtils.parseStatements(sqlMysql, JdbcConstants.MYSQL);
        for(SQLStatement statement:statementList){
            if(statement instanceof MySqlCreateTableStatement) {//创建表
                MySqlCreateTableStatement createTableDDl=(MySqlCreateTableStatement)statement;
                List<SQLTableElement> listColumns=  createTableDDl.getTableElementList();
                for(SQLTableElement column:listColumns){
                    if(column instanceof MySqlPrimaryKey){
                        System.out.println(column);
                        MySqlPrimaryKey primaryKey=(MySqlPrimaryKey)column;
                        List<SQLSelectOrderByItem> listPrimaryKey= primaryKey.getColumns();
                        for(SQLSelectOrderByItem item:listPrimaryKey){
                            if(item.getExpr() instanceof SQLIdentifierExpr){
                                SQLIdentifierExpr identifierExpr=(SQLIdentifierExpr)item.getExpr();
                                System.out.println(identifierExpr.getName().toLowerCase());
                            }
                        }
                    }
                    if(column instanceof SQLColumnDefinition){
                        SQLColumnDefinition currentItems=(SQLColumnDefinition)column;
                        String columnName=currentItems.getNameAsString();

                        System.out.println(columnName);
                        System.out.println(currentItems.toString());

                        if(currentItems.toString()!=null && currentItems.toString().contains("PRIMARY KEY")){
                            System.out.println(currentItems.toString());
                        }
                    }
                }
            }
        }
    }





}
