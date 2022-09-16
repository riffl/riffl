package io.riffl.source;

import io.riffl.config.Source;
import io.riffl.utils.FilesystemUtils;
import io.riffl.utils.TableHelper;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
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
  private final StreamTableEnvironment tableEnv;

  public SourceStream(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
    this.env = env;
    this.tableEnv = tableEnv;
    logger.info(this.tableEnv.getCurrentCatalog());
  }

  public Map<Source, DataStream<Row>> build(List<Source> config) {
    return config.stream()
        .map(
            source -> {
              Path definitionPath = new Path(source.getCreateUri());
              tableEnv.executeSql(FilesystemUtils.openFileAsString(definitionPath));
              ObjectIdentifier sourceId =
                  TableHelper.getCreateTableIdentifier(definitionPath, env, tableEnv);

              Table map;
              if (source.getMapUri() != null) {
                Path mapPath = new Path(source.getMapUri());
                map = tableEnv.sqlQuery(FilesystemUtils.openFileAsString(mapPath));

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
              return Map.entry(source, tableEnv.from(sourceId.asSummaryString()));
            })
        .collect(Collectors.toMap(Entry::getKey, e -> tableEnv.toDataStream(e.getValue())));
  }
}
