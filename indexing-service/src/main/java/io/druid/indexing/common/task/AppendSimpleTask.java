package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.emitter.EmittingLogger;

import org.joda.time.Interval;

import java.io.IOException;
import java.util.List;

import io.druid.indexing.common.actions.TaskActionToolbox;
import io.druid.timeline.DataSegment;

public class AppendSimpleTask extends AppendTask {
    private static final EmittingLogger log = new EmittingLogger(AppendSimpleTask.class);

    @JsonCreator
    public AppendSimpleTask(@JsonProperty("id") String id,
                            @JsonProperty("dataSource") String dataSource,
                            @JsonProperty("interval") Interval interval,
                            @JacksonInject TaskActionToolbox toolbox) {
        super(id,
                dataSource,
                getSegments(dataSource, interval, toolbox));
        log.info("To append segments: %s", getSegments());
        this.getType();
    }

    public AppendTask toAppendTask() {
        return new AppendTask(getId(), getDataSource(), getSegments());
    }

    private static List<DataSegment> getSegments(String dataSource, Interval interval, TaskActionToolbox toolbox) {
        try {
            return toolbox.getIndexerMetadataStorageCoordinator().getUsedSegmentsForInterval(dataSource, interval);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
