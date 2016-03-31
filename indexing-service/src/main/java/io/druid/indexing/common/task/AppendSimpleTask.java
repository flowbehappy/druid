package io.druid.indexing.common.task;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.emitter.EmittingLogger;

import org.joda.time.Interval;

import java.io.IOException;
import java.util.List;

import javax.annotation.Nullable;

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
        List<String> segmentIds = Lists.transform(getSegments(), new Function<DataSegment, String>() {
            @Nullable
            @Override
            public String apply(@Nullable DataSegment input) {
                return input.getIdentifier();
            }
        });
        log.info("To append segments: %s", segmentIds);
        this.getType();
    }

    @Override
    public String getType()
    {
        return "append_simple";
    }

    @JsonIgnore
    @Override
    public List<DataSegment> getSegments() {
        // Too fucking large for zk node!
        return super.getSegments();
    }

    @JsonProperty("interval")
    @Override
    public Interval getInterval() {
        return super.getInterval();
    }

    @Override
    public String toString()
    {

        return Objects.toStringHelper(this)
                .add("id", getId())
                .add("dataSource", getDataSource())
                .add("interval", getInterval())
                .add("segments", getSegments().size())
                .toString();
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
