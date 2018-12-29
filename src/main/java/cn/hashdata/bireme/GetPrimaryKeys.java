package cn.hashdata.bireme;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 *
 *
 * @author zuanqi
 */
public class GetPrimaryKeys {
  private static Logger logger = LogManager.getLogger("Bireme." + GetPrimaryKeys.class);

  public static Map<String, List<String>> getPrimaryKeys(
      HashMap<String, String> tableMap, Connection conn) throws Exception {
    Statement statement = null;
    ResultSet resultSet = null;
    Map<String, List<String>> table_map = new HashMap<>();
    String[] strArray;
    StringBuilder sb = new StringBuilder();
    StringBuilder dbNameSb=new StringBuilder();
    dbNameSb.append("(");
    sb.append("(");
    Set<String> dbNameSet=new HashSet<>();
    for (String fullname : tableMap.values()) {
      strArray = fullname.split("\\.");
      sb.append("'").append(strArray[1].replaceAll("\"","")).append("',");
      dbNameSet.add(strArray[0]);
    }
    for(String db:dbNameSet){
        dbNameSb.append("'").append(db).append("',");
    }
    String tableList = sb.toString().substring(0, sb.toString().length() - 1) + ")";
    String dbNameList= dbNameSb.toString().substring(0,dbNameSb.toString().length()-1) + ")";
    String prSql = "SELECT NULL AS TABLE_CAT, "
        + "n.nspname  AS TABLE_SCHEM, "
        + "ct.relname AS TABLE_NAME, "
        + "a.attname  AS COLUMN_NAME, "
        + "(i.keys).n AS KEY_SEQ, "
        + "ci.relname AS PK_NAME "
        + "FROM pg_catalog.pg_class ct JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid) "
        + "JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid) "
        + "JOIN ( SELECT i.indexrelid, i.indrelid, i.indisprimary, information_schema._pg_expandarray(i.indkey) AS KEYS FROM pg_catalog.pg_index i) i ON (a.attnum = (i.keys).x AND a.attrelid = i.indrelid) "
        + "JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) WHERE TRUE AND n.nspname in "+dbNameList+" AND ct.relname in "
        + tableList + " AND i.indisprimary ORDER BY TABLE_NAME, pk_name, key_seq";
    try {
      statement = conn.createStatement();
      long startTime=System.currentTimeMillis();
      resultSet = statement.executeQuery(prSql);
      logger.info("---------------execute------------------sql----------time:"+(System.currentTimeMillis() - startTime) +"ms");
      while (resultSet.next()) {
        String tableName = resultSet.getString("TABLE_NAME");
        String dbName = resultSet.getString("TABLE_SCHEM");
        if(StringUtils.isNotBlank(tableName)){
            tableName = "\""+tableName+"\"";
        }
        String fullTableName=dbName+"."+tableName;
        if (table_map.containsKey(fullTableName)) {
          List<String> strings = table_map.get(fullTableName);
          strings.add(resultSet.getString("COLUMN_NAME"));
        } else {
          List<String> multiPKList = new ArrayList<>();
          multiPKList.add(resultSet.getString("COLUMN_NAME"));
          table_map.put(fullTableName, multiPKList);
        }
      }

      if (table_map.size() != tableMap.size()) {
        String message = "Greenplum table and MySQL table size are inconsistent!----prSql:"+prSql+" ,table_map:"+table_map.size()+",tableMap:"+tableMap.size();
        throw new BiremeException(message);
      } else {
        logger.info("MySQL、Greenplum table check completed, the state is okay！");
      }

      if (table_map.size() != tableMap.size()) {
        String message = "some tables do not have primary keys！";
        throw new BiremeException(message);
      } else {
        logger.info("Greenplum table primary key check is completed, the state is okay！");
      }

    } catch (SQLException e) {
      try {
        statement.close();
        conn.close();
      } catch (SQLException ignore) {
          logger.debug("非阻碍性",ignore);
      }
      String message = "Could not get PrimaryKeys";
      throw new BiremeException(message, e);
    }

    return table_map;
  }


    public static Map<String, List<String>> getRefulshPrimaryKeys(
            HashMap<String, String> tableMap, Connection conn) throws BiremeException {
        Statement statement = null;
        ResultSet resultSet = null;
        Map<String, List<String>> table_map = new HashMap<>();
        String[] strArray;
        StringBuilder sb = new StringBuilder();
        StringBuilder dbNameSb=new StringBuilder();
        dbNameSb.append("(");
        sb.append("(");
        Set<String> dbNameSet=new HashSet<>();
        for (String fullname : tableMap.values()) {
            strArray = fullname.split("\\.");
            sb.append("'").append(strArray[1].replaceAll("\"","")).append("',");
            dbNameSet.add(strArray[0]);
        }
        for(String db:dbNameSet){
            dbNameSb.append("'").append(db).append("',");
        }
        String tableList = sb.toString().substring(0, sb.toString().length() - 1) + ")";
        String dbNameList= dbNameSb.toString().substring(0,dbNameSb.toString().length()-1) + ")";
        String prSql = "SELECT NULL AS TABLE_CAT, "
                + "n.nspname  AS TABLE_SCHEM, "
                + "ct.relname AS TABLE_NAME, "
                + "a.attname  AS COLUMN_NAME, "
                + "(i.keys).n AS KEY_SEQ, "
                + "ci.relname AS PK_NAME "
                + "FROM pg_catalog.pg_class ct JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid) "
                + "JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid) "
                + "JOIN ( SELECT i.indexrelid, i.indrelid, i.indisprimary, information_schema._pg_expandarray(i.indkey) AS KEYS FROM pg_catalog.pg_index i) i ON (a.attnum = (i.keys).x AND a.attrelid = i.indrelid) "
                + "JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) WHERE TRUE AND n.nspname in "+dbNameList+" AND ct.relname in "
                + tableList + " AND i.indisprimary ORDER BY TABLE_NAME, pk_name, key_seq";
        try {
            statement = conn.createStatement();
            long startTime=System.currentTimeMillis();
            resultSet = statement.executeQuery(prSql);
            logger.info("---------------execute---------reflush---------sql----------time:"+(System.currentTimeMillis() - startTime) +"ms");
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                String dbName = resultSet.getString("TABLE_SCHEM");
                if(StringUtils.isNotBlank(tableName)){
                    tableName = "\""+tableName+"\"";
                }
                String fullTableName=dbName+"."+tableName;
                if (table_map.containsKey(fullTableName)) {
                    List<String> strings = table_map.get(fullTableName);
                    strings.add(resultSet.getString("COLUMN_NAME"));
                } else {
                    List<String> multiPKList = new ArrayList<>();
                    multiPKList.add(resultSet.getString("COLUMN_NAME"));
                    table_map.put(fullTableName, multiPKList);
                }
            }
            if (table_map.size() != tableMap.size()) {
                String message = "Greenplum table and MySQL table size are inconsistent!----prSql:"+prSql+" ,table_map:"+table_map.size()+",tableMap:"+tableMap.size();
                throw new BiremeException(message);
            } else {
                logger.info("MySQL、Greenplum table check completed, the state is okay！");
            }

        } catch (SQLException e) {
            String message = "Could not get PrimaryKeys:+"+prSql;
            throw new BiremeException(message, e);
        }finally {
            if(statement!=null){
                try {
                    statement.close();
                } catch (SQLException e) {
                    throw new BiremeException("---------更新表结构异常----------------",e);
                }
            }
        }
        return table_map;
    }
}
