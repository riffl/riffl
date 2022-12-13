package io.riffl.sink;

import io.riffl.config.Sink;
import io.riffl.sink.allocation.TaskAllocation;
import io.riffl.sink.row.TasksAssignment;
import io.riffl.sink.row.tasks.TaskAssigner;
import io.riffl.sink.row.tasks.TaskAssignerDefaultFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Partitioner extends ProcessFunction<Row, Tuple2<Row, Integer>> {

  private static final Logger logger = LoggerFactory.getLogger(Partitioner.class);

  private final TaskAssignerDefaultFactory taskAssignerFactory;

  private TaskAssigner taskAssigner;

  private final Sink sink;
  private final TaskAllocation taskAllocation;

  public Partitioner(
      Sink sink, TaskAssignerDefaultFactory taskAssignerFactory, TaskAllocation taskAllocation) {

    this.sink = sink;
    this.taskAssignerFactory = taskAssignerFactory;
    this.taskAllocation = taskAllocation;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    taskAllocation.configure();
    taskAssigner =
        taskAssignerFactory.create(sink, taskAllocation.getSinkTasks(sink), new TasksAssignment());
    logger.info("Parallelism {}", getRuntimeContext().getNumberOfParallelSubtasks());
  }

  @Override
  public void processElement(
      Row row,
      ProcessFunction<Row, Tuple2<Row, Integer>>.Context ctx,
      Collector<Tuple2<Row, Integer>> out) {
    out.collect(new Tuple2<>(row, taskAssigner.getTask(row, taskAssigner.getKey(row))));
  }
}
