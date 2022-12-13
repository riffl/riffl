package io.riffl.sink.row;

import java.lang.reflect.Type;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

public class RowKeyTypeInfo extends TypeInfoFactory<RowKey> {

  @Override
  public TypeInformation<RowKey> createTypeInfo(
      Type t, Map<String, TypeInformation<?>> genericParameters) {

    return Types.POJO(RowKey.class, Map.of("key", Types.LIST(Types.INT)));
  }
}
