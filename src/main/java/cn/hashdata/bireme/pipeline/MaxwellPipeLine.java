package cn.hashdata.bireme.pipeline;

import java.util.Arrays;
import java.util.HashMap;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import cn.hashdata.bireme.BiremeException;
import cn.hashdata.bireme.BiremeUtility;
import cn.hashdata.bireme.Context;
import cn.hashdata.bireme.Record;
import cn.hashdata.bireme.Row;
import cn.hashdata.bireme.Table;
import cn.hashdata.bireme.Row.RowType;

/**
 * {@code MaxwellPipeLine} is a kind of {@code KafkaPipeLine} whose change data coming from Maxwell.
 *
 * @author yuze
 *
 */
public class MaxwellPipeLine extends KafkaPipeLine {
  public MaxwellPipeLine(Context cxt, SourceConfig conf, int id) {
    super(cxt, conf, "Maxwell-" + conf.name + "-" + conf.topic + "-" + id);
    consumer.subscribe(Arrays.asList(conf.topic));
    logger = LogManager.getLogger("Bireme." + myName);
    logger.info("Create new Maxwell Pipeline. Name: {}", myName);
  }

  @Override
  public Transformer createTransformer() {
    return new MaxwellTransformer();
  }

  /**
   * {@code MaxwellChangeTransformer} is a type of {@code Transformer}. It is used to transform data
   * to {@code Row} from <B>Maxwell</B> data source.
   *
   * @author yuze
   *
   */
  class MaxwellTransformer extends KafkaTransformer {
    HashMap<String, String> tableMap;

    public MaxwellTransformer() {
      super();
      tableMap = conf.tableMap;
    }

    private String getMappedTableName(MaxwellRecord record) {
      return cxt.tableMap.get(record.dataSource + "." + record.database + "." + record.table);
    }

    private String getOriginTableName(MaxwellRecord record) {
      return record.dataSource + "." + record.database + "." + record.table;
    }

    private boolean filter(MaxwellRecord record) {
      String fullTableName = record.dataSource + "." + record.database + "." + record.table;

      if (!tableMap.containsKey(fullTableName)) {
        return true;
      }

      return false;
    }

    @Override
    protected byte[] decodeToBinary(String data) {
      byte[] decoded = null;
      decoded = Base64.decodeBase64(data);
      return decoded;
    }

    @Override
    protected String decodeToBit(String data, int precision) {
      String binaryStr = Integer.toBinaryString(Integer.valueOf(data));
      String flag="%" + precision + "s";
      return String.format(flag, binaryStr).replace(' ', '0');
    }

    @Override
    public boolean transform(ConsumerRecord<String, String> change, Row row)
        throws BiremeException {
      String value=  change.value();
      if(StringUtils.isBlank(value)){
          return false;
      }
      MaxwellRecord record = new MaxwellRecord(value);

      if (filter(record)) {
        return false;
      }

      Table table = cxt.tablesInfo.get(getMappedTableName(record));

      row.type = record.type;
      row.produceTime = record.produceTime;

      if(row.type == RowType.INSERT || row.type == RowType.UPDATE || row.type == RowType.DELETE ){
          row.originTable = getOriginTableName(record);
          row.mappedTable = getMappedTableName(record);
          row.keys = formatColumns(record, table, table.keyNames, false);
      }

      if (row.type == RowType.INSERT || row.type == RowType.UPDATE) {
        row.tuple = formatColumns(record, table, table.columnName, false);
      }
      if (row.type == RowType.UPDATE) {
        row.oldKeys = formatColumns(record, table, table.keyNames, true);

        if (row.keys.equals(row.oldKeys)) {
          row.oldKeys = null;
        }
      }

      // -----------------------------------------------

      if (row.type == RowType.TABLE_ALTER){//新增，删除，修改 列与索引与 修改表名
           row.pgSql = MysqlToPgDdlUtil.tableAlter(RowType.TABLE_ALTER,record);
           row.originTable = getOriginTableName(record);
           row.mappedTable = getMappedTableName(record);
      }
      if (row.type == RowType.TABLE_CREATE){//仅创建表
          row.pgSql = MysqlToPgDdlUtil.tableAlter(RowType.TABLE_CREATE,record);
      }
      if (row.type == RowType.TABLE_DROP){//仅删除表
          row.pgSql = MysqlToPgDdlUtil.tableAlter(RowType.TABLE_DROP,record);
          row.originTable = getOriginTableName(record);
          row.mappedTable = getMappedTableName(record);
      }
      if (row.type == RowType.DATABASE_CREATE){//创建库
          row.pgSql = MysqlToPgDdlUtil.tableAlter(RowType.DATABASE_CREATE,record);
      }
      if (row.type == RowType.DATABASE_DROP){//删除库
          row.pgSql = MysqlToPgDdlUtil.tableAlter(RowType.DATABASE_CREATE,record);
      }
      return true;
    }

    class MaxwellRecord implements Record {
      public String dataSource;
      public String database;
      public String table;
      public Long produceTime;
      public RowType type;
      public JsonObject data;
      public JsonObject old;
      public JsonObject def;//alter-table 时
      public String sql;//alter-table 时

      public MaxwellRecord(String changeValue) {
        JsonParser jsonParser = new JsonParser();
        JsonObject value = jsonParser.parse(changeValue).getAsJsonObject();
        String typeDb=  value.get("type").getAsString();
        this.dataSource = getPipeLineName();
        this.database = value.has("database") ? value.get("database").getAsString() : "";
        this.table = value.has("table") ? value.get("table").getAsString():"";
        this.produceTime = value.get("ts").getAsLong() * 1000;
        if (value.has("old") && !value.get("old").isJsonNull()) {
            this.old = value.get("old").getAsJsonObject();
        }
        if(value.has("data") && !value.get("data").isJsonNull()){
            this.data = value.get("data").getAsJsonObject();
        }
        if(value.has("def") && !value.get("def").isJsonNull()){
            this.def = value.get("def").getAsJsonObject();
        }
        if(value.has("sql")){
            this.sql = value.get("sql").getAsString();
        }

        switch (typeDb) {
          case "insert":
              type = RowType.INSERT;
              break;
          case "update":
              type = RowType.UPDATE;
              break;
          case "delete":
              type = RowType.DELETE;
              break;
          case "table-drop":
              type = RowType.TABLE_DROP;
              break;
          case "table-create":
              type = RowType.TABLE_CREATE;
              break;
          case "table-alter":
              type = RowType.TABLE_ALTER;
              break;
          case "database-create":
              type = RowType.DATABASE_CREATE;
              break;
          case "database-drop":
              type = RowType.DATABASE_DROP;
              break;
        }
      }

      @Override
      public String getField(String fieldName, boolean oldValue) throws BiremeException {
        String field = null;

        if (oldValue) {
          try {
            field = BiremeUtility.jsonGetIgnoreCase(old, fieldName);
            return field;
          } catch (BiremeException ignore) {
              logger.debug("非阻碍性",ignore);
          }
        }

        return BiremeUtility.jsonGetIgnoreCase(data, fieldName);
      }
    }
  }
}
