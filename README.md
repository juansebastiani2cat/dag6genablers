# airflow
apache airflow test

```yaml
from(bucket: "metrics")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) =>
              r["_measurement"] == "opennebula_libvirt_memory_available_bytes" or
              r["_measurement"] == "opennebula_libvirt_memory_maximum_bytes" or
              r["_measurement"] == "opennebula_libvirt_cpu_seconds_total" or
              r["_measurement"] == "opennebula_libvirt_cpu_system_seconds_total" or
              r["_measurement"] == "opennebula_libvirt_cpu_user_seconds_total" 
            )
  |> filter(fn: (r) => r["_field"] == "gauge")
  //|> filter(fn: (r) => r["one_vm_id"] == "1")
  |> aggregateWindow(every: 10m, fn: mean, createEmpty: false)
  |> yield(name: "mean")
```


    query = f'''
                from(bucket: "{bucket}")
                |> range(start: {timeRangeStart}, stop: {timeRangeStop})
                |> filter(fn: (r) =>
                        r["_measurement"] == "opennebula_libvirt_memory_available_bytes" or
                        r["_measurement"] == "opennebula_libvirt_memory_maximum_bytes" or
                        r["_measurement"] == "opennebula_libvirt_cpu_seconds_total" or
                        r["_measurement"] == "opennebula_libvirt_cpu_system_seconds_total" or
                        r["_measurement"] == "opennebula_libvirt_cpu_user_seconds_total" 
                         )
                |> filter(fn: (r) => r["_field"] == "gauge")
                |> aggregateWindow(every: {windowPeriod}, fn: mean, createEmpty: false)
                |> yield(name: "mean")
                '''