from dask.distributed import LocalCluster
client = LocalCluster().get_client()

input_path = "/media/kakn/storage/filtered/manual/testdata"
output_file = "/media/kakn/storage/filtered/low_v1.parquet"
range_start = 1736680895
range_end = 1736684707

import datetime
range_start = datetime.datetime.fromtimestamp(range_start)
range_end = datetime.datetime.fromtimestamp(range_end)


input_egress_path = input_path + "/egress"
input_ingress_path = input_path + "/ingress"

import os
import dask.dataframe as dd

ingress_files = list(map( lambda x: os.path.join(input_ingress_path, x), [pos_json for pos_json in os.listdir(input_ingress_path) if pos_json.endswith('.json')]))
egress_files = list(map( lambda x: os.path.join(input_egress_path, x),[pos_json for pos_json in os.listdir(input_egress_path) if pos_json.endswith('.json')]))

structure = {
        "cng_deviceId": "str",
        "timestamp": "datetime64[ns]",
        "timePeriodStart": "datetime64[ns]",
        "messageTimestamp": "datetime64[ns]",
        "firmwareVersion": "str",
        "temperature": "float64",
        "powerState": "int8",
        "coolerState": "int8",
        "doorOpenCount": "int8",
        "doorCloseCount": "int8",
        "doorOpenTime": "int16",
        "batteryLevel": "int8",
        "latitude": "float64",
        "longitude": "float64",
        "locationType": "str",
        "locationConfidence": "float64",
        "wifiCount": "int8",
        "mobileCellId": "str",
        "mobileCellType": "str",
        "mobileMNC": "int8",
        "mobileMCC": "int16",
        "mobileRSSI": "int8",
        "mobileLac": "int8",
        "messageType": "int8",
        "plausibilityState": "int8",
        "traceId": "int32",
        "deduplicationHash": "int64",
        "kafkaTime": "datetime64[ns]", 
    }


ingress_frame = dd.read_json(
    ingress_files,
    lines=True,
    blocksize=620,
    meta=structure
    )

egress_frame = dd.read_json(
    egress_files,
    lines=True,
    blocksize=620,
    meta=structure
    )

ingress_frame = ingress_frame \
    .drop(["cng_deviceId", "timestamp", "timePeriodStart", "messageTimestamp", "firmwareVersion", "temperature", "powerState", "coolerState", "doorOpenCount", "doorCloseCount", "doorOpenTime", "batteryLevel", "latitude", "longitude", "locationType", "locationConfidence", "wifiCount", "mobileCellId", "mobileCellType", "mobileMNC", "mobileMCC", "mobileRSSI", "mobileLac", "messageType", "plausibilityState"], axis=1) \
    .query("kafkaTime >= @range_start and kafkaTime <= @range_end", local_dict={"range_start": range_start, "range_end": range_end}) \

egress_frame = egress_frame \
    .drop(["cng_deviceId", "timestamp", "timePeriodStart", "messageTimestamp", "firmwareVersion", "temperature", "powerState", "coolerState", "doorOpenCount", "doorCloseCount", "doorOpenTime", "batteryLevel", "latitude", "longitude", "locationType", "locationConfidence", "wifiCount", "mobileCellId", "mobileCellType", "mobileMNC", "mobileMCC", "mobileRSSI", "mobileLac", "messageType", "plausibilityState"], axis=1) \
    .query("kafkaTime >= @range_start and kafkaTime <= @range_end", local_dict={"range_start": range_start, "range_end": range_end}) \

combined_frame = egress_frame.merge(
    ingress_frame, 
    on=["traceId", "deduplicationHash"], 
    how="inner", 
    suffixes=('_egress', '_ingress')
    )


combined_frame = combined_frame.assign(latency=lambda x: (x.kafkaTime_egress - x.kafkaTime_ingress).astype('int64'))

print("Combined frame columns: ", combined_frame.dtypes)

mean_latency = combined_frame.latency.mean().compute()
#median_latency = combined_frame.latency.median_approximate()
#std_latency = combined_frame.latency.std()
#max_latency = combined_frame.latency.max()
#min_latency = combined_frame.latency.min()
#percentile_95_latency = combined_frame.latency.quantile(0.95)
#percentile_99_latency = combined_frame.latency.quantile(0.99)

#dd.compute(mean_latency, median_latency, std_latency, max_latency, min_latency, percentile_95_latency, percentile_99_latency)

print("Mean latency: ", mean_latency)
#print("Median latency: ", median_latency)
#print("Std latency: ", std_latency)
#print("Max latency: ", max_latency)
#print("Min latency: ", min_latency)
#print("95th percentile latency: ", percentile_95_latency)
#print("99th percentile latency: ", percentile_99_latency)
