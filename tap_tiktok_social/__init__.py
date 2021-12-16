#!/usr/bin/env python3
import os
import json
import math
import singer
import backoff
import requests
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from singer.transform import transform
from datetime import datetime, timedelta


REQUIRED_CONFIG_KEYS = ["open_id", "start_date", "refresh_token", "client_key"]
HOST = "https://open-api.tiktok.com"
END_POINTS = {
    "user_info": "/user/info/",
    "video_list": "/video/list/"
}
LOGGER = singer.get_logger()


class TiktokRateLimitError(Exception):
    def __init__(self, msg):
        self.msg = msg
        super().__init__(self.msg)


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def get_attr_for_auto_inclusion(tap_stream_id):
    auto_inclusion = {
        "video_list": ["create_time"]
    }
    return auto_inclusion.get(tap_stream_id, [])


def get_key_properties(stream_name):
    key_properties = {
        "user_info": ["open_id"],
        "video_list": ["id"]
    }
    return key_properties.get(stream_name, [])


def create_metadata_for_report(schema, tap_stream_id):
    key_properties = get_key_properties(tap_stream_id)
    auto_inclusion = get_attr_for_auto_inclusion(tap_stream_id)

    mdata = [{"breadcrumb": [], "metadata": {"inclusion": "available", "forced-replication-method": "FULL_TABLE"}}]

    if key_properties:
        mdata[0]["metadata"]["table-key-properties"] = key_properties

    if tap_stream_id == "video_list":
        mdata[0]["metadata"]["forced-replication-method"] = "INCREMENTAL"
        mdata[0]["metadata"]["valid-replication-keys"] = ["create_time"]

    for key in schema.properties:
        if "object" in schema.properties.get(key).type:
            for prop in schema.properties.get(key).properties:
                inclusion = "automatic" if prop in key_properties + auto_inclusion else "available"
                mdata.extend([{
                    "breadcrumb": ["properties", key, "properties", prop],
                    "metadata": {"inclusion": inclusion}
                }])
        else:
            inclusion = "automatic" if key in key_properties + auto_inclusion else "available"
            mdata.append({"breadcrumb": ["properties", key], "metadata": {"inclusion": inclusion}})

    return mdata


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = create_metadata_for_report(schema, stream_id)
        key_properties = get_key_properties(stream_id)
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata
            )
        )
    return Catalog(streams)


def _refresh_token(config):
    data = {
        'client_key': config['client_key'],
        'grant_type': 'refresh_token',
        'refresh_token': config['refresh_token']
    }
    url = 'https://open-api.tiktok.com/oauth/refresh_token/'
    response = requests.post(url, data=data)
    return response.json()["data"]


def refresh_access_token_if_expired(config):
    # if [expires_at not exist] or if [exist and less then current time] then it will update the token
    if config.get('expires_at') is None or config.get('expires_at') < datetime.utcnow():
        res = _refresh_token(config)
        config["access_token"] = res["access_token"]
        config["open_id"] = res["open_id"]
        config["refresh_token"] = res["refresh_token"]
        config["expires_at"] = datetime.utcnow() + timedelta(seconds=res["expires_in"])
        return True
    return False


@backoff.on_exception(backoff.expo, TiktokRateLimitError, max_tries=5, factor=2)
@utils.ratelimit(1, 1)
def request_data(payload, config, headers, endpoint):
    url = HOST + endpoint

    if refresh_access_token_if_expired(config) or not payload.get("access_token"):
        payload["access_token"] = config["access_token"]

    response = requests.post(url, data=json.dumps(payload), headers=headers)
    res = response.json()
    if response.status_code == 429:
        raise TiktokRateLimitError(response.text)
    elif response.status_code != 200 or res["error"]["code"]:
        if res["error"]["message"] == '':
            res["error"][
                "message"] = "Make sure you provide all needed permissions while authenticating for TikTok. <read " \
                             "user info, read public videos> "
        raise Exception(str(res["error"]))
    data = res.get("data", {})
    return data


def get_selected_attrs(stream):
    list_attrs = list()
    for md in stream.metadata:
        if md["breadcrumb"]:
            if md["metadata"].get("selected", False) or md["metadata"].get("inclusion") == "automatic":
                list_attrs.append(md["breadcrumb"][-1])

    return list_attrs


def sync_user_info(config, state, stream):
    mdata = metadata.to_map(stream.metadata)
    schema = stream.schema.to_dict()

    singer.write_schema(
        stream_name=stream.tap_stream_id,
        schema=schema,
        key_properties=stream.key_properties,
    )
    endpoint = END_POINTS[stream.tap_stream_id]

    headers = {"Content-Type": "application/json"}
    data = {
        "open_id": config["open_id"],
        "fields": get_selected_attrs(stream)
    }

    record = request_data(data, config, headers, endpoint)

    with singer.metrics.record_counter(stream.tap_stream_id) as counter:
        for row in record.values():
            # Type Conversation and Transformation
            transformed_data = transform(row, schema, metadata=mdata)

            # write one or more rows to the stream:
            singer.write_records(stream.tap_stream_id, [transformed_data])
            counter.increment()


def sync_streams(config, state, stream):
    bookmark_column = "cursor"
    mdata = metadata.to_map(stream.metadata)
    schema = stream.schema.to_dict()

    singer.write_schema(
        stream_name=stream.tap_stream_id,
        schema=schema,
        key_properties=stream.key_properties,
    )
    endpoint = END_POINTS[stream.tap_stream_id]
    headers = {"Content-Type": "application/json"}
    start_cursor = singer.get_bookmark(state, stream.tap_stream_id, bookmark_column) \
        if state.get("bookmarks", {}).get(stream.tap_stream_id) else \
        datetime.strptime(config["start_date"], "%Y-%m-%d").timestamp() * 1000

    has_more = True
    data = {
        "open_id": config["open_id"],
        "cursor": math.trunc(start_cursor),
        "fields": get_selected_attrs(stream)
    }
    while has_more:
        res = request_data(data, config, headers, endpoint)

        videos = res.get("videos", [])
        has_more = res.get("has_more", False)
        data["cursor"] = res.get("cursor")

        with singer.metrics.record_counter(stream.tap_stream_id) as counter:
            for row in videos:
                # Type Conversation and Transformation
                transformed_data = transform(row, schema, metadata=mdata)

                # write one or more rows to the stream:
                singer.write_records(stream.tap_stream_id, [transformed_data])
                counter.increment()

        state = singer.write_bookmark(state, stream.tap_stream_id, bookmark_column, data["cursor"])
        singer.write_state(state)


def sync(config, state, catalog):
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        if stream.tap_stream_id == "user_info":
            sync_user_info(config, state, stream)
        else:
            sync_streams(config, state, stream)
    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        state = args.state or {}
        sync(args.config, state, catalog)


if __name__ == "__main__":
    main()

