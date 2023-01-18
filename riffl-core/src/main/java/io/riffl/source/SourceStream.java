package io.riffl.source;

import io.riffl.config.Source;
import io.riffl.utils.TableHelper;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceStream {

  private static final Logger logger = LoggerFactory.getLogger(SourceStream.class);

  private static final String TEMP_TABLE_POSTFIX = "-riffl-tmp";

  private final StreamExecutionEnvironment env;

  public SourceStream(StreamExecutionEnvironment env) {
    this.env = env;
  }

  public Map<Source, DataStream<Row>> build(List<Source> sources) {
    return sources.stream()
        .map(
            source -> {
              var tableEnv = StreamTableEnvironment.create(env);
              if (source.getParallelism() != null) {
                tableEnv
                    .getConfig()
                    .getConfiguration()
                    .setInteger(
                        ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM,
                        source.getParallelism());
              }
              tableEnv.executeSql(source.getCreate());
              ObjectIdentifier sourceId =
                  TableHelper.getCreateTableIdentifier(source.getCreate(), env, tableEnv);
              logger.info("Created table: {}", sourceId.asSummaryString());
              Table map;
              if (source.getMap() != null) {
                map = tableEnv.sqlQuery(source.getMap());

                if (tableEnv.getCatalog(sourceId.getCatalogName()).isPresent()) {
                  Catalog catalog = tableEnv.getCatalog(sourceId.getCatalogName()).get();
                  try {
                    catalog.renameTable(
                        sourceId.toObjectPath(),
                        sourceId.getObjectName() + TEMP_TABLE_POSTFIX,
                        false);
                  } catch (TableNotExistException | TableAlreadyExistException e) {
                    throw new RuntimeException(e);
                  }
                } else {
                  throw new RuntimeException(
                      MessageFormat.format(
                          "Non-existent catalog name: {0}", sourceId.getCatalogName()));
                }
              } else {
                map = tableEnv.from(sourceId.asSummaryString());
              }

              tableEnv.createTemporaryView(
                  sourceId.asSummaryString(),
                  source.getRebalance()
                      ? tableEnv.toDataStream(map).rebalance()
                      : tableEnv.toDataStream(map));
              return Map.entry(
                  source, tableEnv.toDataStream(tableEnv.from(sourceId.asSummaryString())));
            })
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
  }
}
