/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexing.overlord.http;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteSource;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;

import com.fasterxml.jackson.annotation.JsonValue;
import com.metamx.common.logger.Logger;

import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.druid.common.config.JacksonConfigManager;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.actions.TaskActionHolder;
import io.druid.indexing.common.task.AppendSimpleTask;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.TaskQueue;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.TaskRunnerWorkItem;
import io.druid.indexing.overlord.TaskStorageQueryAdapter;
import io.druid.indexing.overlord.autoscaling.ResourceManagementScheduler;
import io.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import io.druid.metadata.EntryExistsException;
import io.druid.tasklogs.TaskLogStreamer;
import io.druid.timeline.DataSegment;

/**
 */
@Path("/druid/indexer/v1")
public class OverlordResource
{
  private static final Logger log = new Logger(OverlordResource.class);

  private final TaskMaster taskMaster;
  private final TaskStorageQueryAdapter taskStorageQueryAdapter;
  private final TaskLogStreamer taskLogStreamer;
  private final JacksonConfigManager configManager;

  private AtomicReference<WorkerBehaviorConfig> workerConfigRef = null;

  @Inject
  public OverlordResource(
      TaskMaster taskMaster,
      TaskStorageQueryAdapter taskStorageQueryAdapter,
      TaskLogStreamer taskLogStreamer,
      JacksonConfigManager configManager
  ) throws Exception
  {
    this.taskMaster = taskMaster;
    this.taskStorageQueryAdapter = taskStorageQueryAdapter;
    this.taskLogStreamer = taskLogStreamer;
    this.configManager = configManager;
  }

  @POST
  @Path("/task")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response taskPost(Task task)
  {
    if(task instanceof AppendSimpleTask){
      task = ((AppendSimpleTask) task).toAppendTask();
    }
    final Task realTask = task;
    return asLeaderWith(
        taskMaster.getTaskQueue(),
        new Function<TaskQueue, Response>()
        {
          @Override
          public Response apply(TaskQueue taskQueue)
          {
            try {
              taskQueue.add(realTask);
              return Response.ok(ImmutableMap.of("task", realTask.getId())).build();
            }
            catch (EntryExistsException e) {
              return Response.status(Response.Status.BAD_REQUEST)
                             .entity(ImmutableMap.of("error", String.format("Task[%s] already exists!", realTask.getId())))
                             .build();
            }
          }
        }
    );
  }

  @GET
  @Path("/leader")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLeader()
  {
    return Response.ok(taskMaster.getLeader()).build();
  }

  @GET
  @Path("/task/{taskid}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTaskPayload(@PathParam("taskid") String taskid)
  {
    return optionalTaskResponse(taskid, "payload", taskStorageQueryAdapter.getTask(taskid));
  }

  @GET
  @Path("/task/{taskid}/status")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTaskStatus(@PathParam("taskid") String taskid)
  {
    return optionalTaskResponse(taskid, "status", taskStorageQueryAdapter.getStatus(taskid));
  }

  @GET
  @Path("/task/{taskid}/segments")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTaskSegments(@PathParam("taskid") String taskid)
  {
    final Set<DataSegment> segments = taskStorageQueryAdapter.getInsertedSegments(taskid);
    return Response.ok().entity(segments).build();
  }

  @POST
  @Path("/task/{taskid}/shutdown")
  @Produces(MediaType.APPLICATION_JSON)
  public Response doShutdown(@PathParam("taskid") final String taskid)
  {
    return asLeaderWith(
        taskMaster.getTaskQueue(),
        new Function<TaskQueue, Response>()
        {
          @Override
          public Response apply(TaskQueue taskQueue)
          {
            taskQueue.shutdown(taskid);
            return Response.ok(ImmutableMap.of("task", taskid)).build();
          }
        }
    );
  }

  @GET
  @Path("/worker")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getWorkerConfig()
  {
    if (workerConfigRef == null) {
      workerConfigRef = configManager.watch(WorkerBehaviorConfig.CONFIG_KEY, WorkerBehaviorConfig.class);
    }

    return Response.ok(workerConfigRef.get()).build();
  }

  @POST
  @Path("/worker")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response setWorkerConfig(
      final WorkerBehaviorConfig workerBehaviorConfig
  )
  {
    if (!configManager.set(WorkerBehaviorConfig.CONFIG_KEY, workerBehaviorConfig)) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }

    log.info("Updating Worker configs: %s", workerBehaviorConfig);

    return Response.ok().build();
  }

  @POST
  @Path("/action")
  @Produces(MediaType.APPLICATION_JSON)
  public <T> Response doAction(final TaskActionHolder<T> holder)
  {
    return asLeaderWith(
        taskMaster.getTaskActionClient(holder.getTask()),
        new Function<TaskActionClient, Response>()
        {
          @Override
          public Response apply(TaskActionClient taskActionClient)
          {
            final Map<String, Object> retMap;

            // It would be great to verify that this worker is actually supposed to be running the task before
            // actually doing the action.  Some ideas for how that could be done would be using some sort of attempt_id
            // or token that gets passed around.

            try {
              final T ret = taskActionClient.submit(holder.getAction());
              retMap = Maps.newHashMap();
              retMap.put("result", ret);
            }
            catch (IOException e) {
              log.warn(e, "Failed to perform task action");
              return Response.serverError().build();
            }

            return Response.ok().entity(retMap).build();
          }
        }
    );
  }

  @GET
  @Path("/waitingTasks")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getWaitingTasks()
  {
    return workItemsResponse(
        new Function<TaskRunner, Collection<? extends TaskRunnerWorkItem>>()
        {
          @Override
          public Collection<? extends TaskRunnerWorkItem> apply(TaskRunner taskRunner)
          {
            // A bit roundabout, but works as a way of figuring out what tasks haven't been handed
            // off to the runner yet:
            final List<Task> activeTasks = taskStorageQueryAdapter.getActiveTasks();
            final Set<String> runnersKnownTasks = Sets.newHashSet(
                Iterables.transform(
                    taskRunner.getKnownTasks(),
                    new Function<TaskRunnerWorkItem, String>()
                    {
                      @Override
                      public String apply(final TaskRunnerWorkItem workItem)
                      {
                        return workItem.getTaskId();
                      }
                    }
                )
            );
            final List<TaskRunnerWorkItem> waitingTasks = Lists.newArrayList();
            for (final Task task : activeTasks) {
              if (!runnersKnownTasks.contains(task.getId())) {
                waitingTasks.add(
                    // Would be nice to include the real created date, but the TaskStorage API doesn't yet allow it.
                    new TaskRunnerWorkItem(
                        task.getId(),
                        SettableFuture.<TaskStatus>create(),
                        new DateTime(0),
                        new DateTime(0)
                    )
                );
              }
            }
            return waitingTasks;
          }
        }
    );
  }

  @GET
  @Path("/pendingTasks")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getPendingTasks()
  {
    return workItemsResponse(
        new Function<TaskRunner, Collection<? extends TaskRunnerWorkItem>>()
        {
          @Override
          public Collection<? extends TaskRunnerWorkItem> apply(TaskRunner taskRunner)
          {
            return taskRunner.getPendingTasks();
          }
        }
    );
  }

  @GET
  @Path("/runningTasks")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRunningTasks()
  {
    return workItemsResponse(
        new Function<TaskRunner, Collection<? extends TaskRunnerWorkItem>>()
        {
          @Override
          public Collection<? extends TaskRunnerWorkItem> apply(TaskRunner taskRunner)
          {
            return taskRunner.getRunningTasks();
          }
        }
    );
  }

  @GET
  @Path("/completeTasks")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCompleteTasks()
  {
    final List<TaskResponseObject> completeTasks = Lists.transform(
        taskStorageQueryAdapter.getRecentlyFinishedTaskStatuses(),
        new Function<TaskStatus, TaskResponseObject>()
        {
          @Override
          public TaskResponseObject apply(TaskStatus taskStatus)
          {
            // Would be nice to include the real created date, but the TaskStorage API doesn't yet allow it.
            return new TaskResponseObject(
                taskStatus.getId(),
                new DateTime(0),
                new DateTime(0),
                Optional.of(taskStatus)
            );
          }
        }
    );
    return Response.ok(completeTasks).build();
  }

  @GET
  @Path("/workers")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getWorkers()
  {
    return asLeaderWith(
        taskMaster.getTaskRunner(),
        new Function<TaskRunner, Response>()
        {
          @Override
          public Response apply(TaskRunner taskRunner)
          {
            return Response.ok(taskRunner.getWorkers()).build();
          }
        }
    );
  }

  @GET
  @Path("/scaling")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getScalingState()
  {
    // Don't use asLeaderWith, since we want to return 200 instead of 503 when missing an autoscaler.
    final Optional<ResourceManagementScheduler> rms = taskMaster.getResourceManagementScheduler();
    if (rms.isPresent()) {
      return Response.ok(rms.get().getStats()).build();
    } else {
      return Response.ok().build();
    }
  }

  @GET
  @Path("/task/{taskid}/log")
  @Produces("text/plain")
  public Response doGetLog(
      @PathParam("taskid") final String taskid,
      @QueryParam("offset") @DefaultValue("0") final long offset
  )
  {
    try {
      final Optional<ByteSource> stream = taskLogStreamer.streamTaskLog(taskid, offset);
      if (stream.isPresent()) {
        return Response.ok(stream.get().openStream()).build();
      } else {
        return Response.status(Response.Status.NOT_FOUND)
                       .entity(
                           "No log was found for this task. "
                           + "The task may not exist, or it may not have begun running yet."
                       )
                       .build();
      }
    }
    catch (Exception e) {
      log.warn(e, "Failed to stream log for task %s", taskid);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  private Response workItemsResponse(final Function<TaskRunner, Collection<? extends TaskRunnerWorkItem>> fn)
  {
    return asLeaderWith(
        taskMaster.getTaskRunner(),
        new Function<TaskRunner, Response>()
        {
          @Override
          public Response apply(TaskRunner taskRunner)
          {
            return Response.ok(
                Lists.transform(
                    Lists.newArrayList(fn.apply(taskRunner)),
                    new Function<TaskRunnerWorkItem, TaskResponseObject>()
                    {
                      @Override
                      public TaskResponseObject apply(TaskRunnerWorkItem workItem)
                      {
                        return new TaskResponseObject(
                            workItem.getTaskId(),
                            workItem.getCreatedTime(),
                            workItem.getQueueInsertionTime(),
                            Optional.<TaskStatus>absent()
                        );
                      }
                    }
                )
            ).build();
          }
        }
    );
  }

  private <T> Response optionalTaskResponse(String taskid, String objectType, Optional<T> x)
  {
    final Map<String, Object> results = Maps.newHashMap();
    results.put("task", taskid);
    if (x.isPresent()) {
      results.put(objectType, x.get());
      return Response.status(Response.Status.OK).entity(results).build();
    } else {
      return Response.status(Response.Status.NOT_FOUND).entity(results).build();
    }
  }

  private <T> Response asLeaderWith(Optional<T> x, Function<T, Response> f)
  {
    if (x.isPresent()) {
      return f.apply(x.get());
    } else {
      // Encourage client to try again soon, when we'll likely have a redirect set up
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
    }
  }

  private static class TaskResponseObject
  {
    private final String id;
    private final DateTime createdTime;
    private final DateTime queueInsertionTime;
    private final Optional<TaskStatus> status;

    private TaskResponseObject(
        String id,
        DateTime createdTime,
        DateTime queueInsertionTime,
        Optional<TaskStatus> status
    )
    {
      this.id = id;
      this.createdTime = createdTime;
      this.queueInsertionTime = queueInsertionTime;
      this.status = status;
    }

    public String getId()
    {
      return id;
    }

    public DateTime getCreatedTime()
    {
      return createdTime;
    }

    public DateTime getQueueInsertionTime()
    {
      return queueInsertionTime;
    }

    public Optional<TaskStatus> getStatus()
    {
      return status;
    }

    @JsonValue
    public Map<String, Object> toJson()
    {
      final Map<String, Object> data = Maps.newLinkedHashMap();
      data.put("id", id);
      if (createdTime.getMillis() > 0) {
        data.put("createdTime", createdTime);
      }
      if (queueInsertionTime.getMillis() > 0) {
        data.put("queueInsertionTime", queueInsertionTime);
      }
      if (status.isPresent()) {
        data.put("statusCode", status.get().getStatusCode().toString());
      }
      return data;
    }
  }
}
