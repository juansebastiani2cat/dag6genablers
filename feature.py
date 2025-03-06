# This is an example feature definition file
from datetime import timedelta
import pandas as pd
from feast import Entity, FeatureService, FeatureView, Field, PushSource, RequestSource
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64, String
# Define an entity for the driver. You can think of an entity as a primary key used to
# fetch features.
driver_opennebula = Entity(name="metrics_edge", join_keys=["one_vm_name"])
driver_stats_source_opennebula = PostgreSQLSource(
    name="metrics__service_opennebula",
    query='''SELECT
                t1._time,
                t1.one_vm_name,
                t1.one_vm_worker,
                t1.value_max_orchestrator_k8s_pod_cpu_utilization,
                t1.value_mean_orchestrator_k8s_pod_cpu_utilization,
                t1.value_min_orchestrator_k8s_pod_cpu_utilization,
                t2.value_mean_rgbd_latency_ms,
                t2.value_mean_fps,
                t2.value_mean_sessions_count
            FROM
                metrics_holo_k8s_1m t1
            JOIN
                metrics_holo_app_1m t2
            ON
                t1._time = t2._time
        ''',
    timestamp_field="_time",
    #created_timestamp_column="created",
)

metrics_holo_view = FeatureView(
    # The unique name of this feature view. Two feature views in a single
    # project cannot have the same name
    name="holo_feature",
    entities=[driver_opennebula],
    ttl=timedelta(days=1),
    # The list of features defined below act as a schema to both define features
    # for both materialization of features into a store, and are used as references
    # during retrieval for building a training dataset or serving features
    schema=[
        Field(name="value_max_orchestrator_k8s_pod_cpu_utilization", dtype=Float32),
        Field(name="value_mean_orchestrator_k8s_pod_cpu_utilization", dtype=Float32),
        Field(name="value_min_orchestrator_k8s_pod_cpu_utilization", dtype=Float32),
        Field(name="value_mean_rgbd_latency_ms", dtype=Float32),
        Field(name="value_mean_fps", dtype=Float32),
        Field(name="value_mean_sessions_count", dtype=Float32),
    ],
    online=True,
    source=driver_stats_source_opennebula,
    # Tags are user defined key/value pairs that are attached to each
    # feature view
    #tags={"team": "ramp"},
)
metrics_holo_service = FeatureService(
    name="holo_feature",
    features=[metrics_holo_view]
)