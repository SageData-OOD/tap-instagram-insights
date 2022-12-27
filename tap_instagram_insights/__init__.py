#!/usr/bin/env python3
import os
import json
import traceback
import facebook
import singer
from datetime import datetime, timedelta, date
from collections import defaultdict
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from singer.transform import transform

REQUIRED_CONFIG_KEYS = ["start_date", "access_token", "fb_object_ids"]
LOGGER = singer.get_logger()


def get_key_properties(stream_id):
    key_properties = {
        "page_insights": ["created_ts", "page_id"],
        "page_overall_metrics": ["created_ts", "page_id"],
        "posts_insights": ["created_ts", "post_id"],
        "posts_mentions": ["tagged_name", "post_id"]
    }

    return key_properties.get(stream_id, [])


def get_bookmark(stream_id):
    """
    Bookmarks for the streams which has incremental sync.
    """
    bookmarks = {
        "page_insights": "created_ts",
        "page_overall_metrics": "created_ts",
        "posts_insights": "created_ts"
    }
    return bookmarks.get(stream_id)


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


def create_metadata_for_report(stream_id, schema, key_properties):
    replication_key = get_bookmark(stream_id)
    mdata = [{"breadcrumb": [], "metadata": {"inclusion": "available", "forced-replication-method": "INCREMENTAL",
                                             "valid-replication-keys": [replication_key]}}]

    if key_properties:
        mdata[0]["metadata"]["table-key-properties"] = key_properties

    if not replication_key:
        mdata[0]["metadata"]["forced-replication-method"] = "FULL_TABLE"
        mdata[0]["metadata"].pop("valid-replication-keys")

    for key in schema.properties:
        # hence, when property is object, we will only consider properties of that object without taking object itself.
        if "object" in schema.properties.get(key).type and schema.properties.get(key).properties:
            inclusion = "available"
            mdata.extend(
                [{"breadcrumb": ["properties", key, "properties", prop],
                  "metadata": {"inclusion": inclusion}} for prop
                 in schema.properties.get(key).properties])
        else:
            inclusion = "automatic" if key in key_properties + [replication_key] else "available"
            mdata.append({"breadcrumb": ["properties", key], "metadata": {"inclusion": inclusion}})

    return mdata


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = create_metadata_for_report(stream_id, schema, get_key_properties(stream_id))
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


def date_today():
    return datetime.today().date()


def date_before_today():
    return date_today() - timedelta(days=1)


def string_to_date(_date):
    return datetime.strptime(_date, "%Y-%m-%dT%H:%M:%S.%fZ").date()


def get_verified_date_to_poll(date_to_poll, stream_id):
    """
    For Page_Insights:
        (follower_count) metric only supports querying data for the last 30 days excluding the current day
        so, if date_to_poll is less than 30 days, it will be changed to the least possible date value (today - 30 days)
    """

    date_to_poll = datetime.strptime(date_to_poll, "%Y-%m-%d")
    if stream_id == "page_insights":
        minimal_date_to_poll = datetime.utcnow() - timedelta(days=30)
        maximum_date_to_poll = datetime.utcnow() - timedelta(days=1)

        date_to_poll = max(date_to_poll, minimal_date_to_poll)
        date_to_poll = min(date_to_poll, maximum_date_to_poll)

    return date_to_poll.date()


def get_end_date(start_date):
    # if end_date bigger than today's date, then end_date = today
    end_date = min(start_date + timedelta(days=30), date.today())

    return end_date


# Query Data
def query_page_info(fb_graph, business_account_id):
    page_info = fb_graph.get_object(id=business_account_id, fields="username,ig_id,followers_count,media_count")
    page_info["created_ts"] = str(datetime.utcnow())
    page_info["business_account_id"] = page_info.pop("id") if "id" in page_info else business_account_id
    page_info["page_name"] = page_info.pop("username")
    page_info["page_id"] = page_info.pop("ig_id")
    return page_info


def query_page_insights(fb_graph, metrics, business_account_id, start_date, end_date):
    page_info = query_page_info(fb_graph, business_account_id)
    all_insights = defaultdict(dict)

    connection_name = "&".join(
        ["insights?period=day&since={}&until={}".format(start_date, end_date),
         "metric={}".format(",".join(metrics))]
    )
    LOGGER.info("connection_name: %s", connection_name)
    page_insights_generator = fb_graph.get_all_connections(id=business_account_id,
                                                           connection_name=connection_name)
    for insights in page_insights_generator:
        # print(insights)
        for stats in insights["values"]:
            all_insights[stats["end_time"]][insights["name"]] = stats["value"]

    for k, v in all_insights.items():
        v["created_ts"] = k
        v["page_name"] = page_info["page_name"]
        v["page_id"] = page_info["page_id"]
        v["business_account_id"] = business_account_id

    return list(all_insights.values())


def query_stories(fb_graph, business_account_id):
    """
    Stories insights are only available for 24 hours.
    """

    stories = []
    connection_name, fields = ("stories", "permalink")
    LOGGER.info("Query connection name: %s", connection_name)

    # this reads all instagram stories
    stories_generator = fb_graph.get_all_connections(
        id=business_account_id, connection_name=connection_name, fields=fields
    )

    for story in stories_generator:
        LOGGER.info("type: %s, id: %s", connection_name, story.get("id"))
        stories.append(story)

    LOGGER.info("Total %s queried: %d", connection_name, len(stories))

    return stories


def query_posts(fb_graph, business_account_id, start_date, end_date):
    posts = []
    connection_name, fields = ("media?since={}&until={}".format(start_date, end_date), "timestamp,permalink")
    LOGGER.info("Query connection name: %s", connection_name)

    # this reads all instagram posts
    posts_generator = fb_graph.get_all_connections(
        id=business_account_id, connection_name=connection_name, fields=fields
    )

    for post in posts_generator:
        LOGGER.info("type: %s, id: %s", connection_name, post.get("id"))
        posts.append(post)

    LOGGER.info("Total %s queried: %d", connection_name, len(posts))

    return posts


def query_posts_insights(fb_graph, business_account_id, start_date, end_date, page_info, all_posts):
    all_posts["media"] = query_posts(fb_graph, business_account_id,
                                     start_date=start_date,
                                     end_date=end_date)

    if end_date >= date_before_today():
        all_posts["stories"] = query_stories(fb_graph, business_account_id)

    return query_post_metrics(fb_graph, page_info, all_posts)


def query_post_metrics(fb_graph, page_info, all_posts):
    queried_posts = []

    for post_type, posts in all_posts.items():
        if not posts:
            continue

        metrics = ["caption", "comments_count", "like_count", "permalink", "timestamp"]
        fields = [] + metrics
        insights = ["impressions", "reach"]

        # query metrics depending on the post type
        if post_type == "media":
            insights += ["engagement", "saved", "shares", "video_views", "total_interactions", "plays"]
        else:  # stories
            insights += ["exits", "replies", "taps_forward", "taps_back"]

        fields.append("insights.date_preset(lifetime).metric({})".format(",".join(insights)))

        renamed_columns = {
            "timestamp": "created_ts",
            "permalink": "post_url",
            "username": "page_name",
            "comments_count": "comments",
            "like_count": "likes"
        }

        # 50 ids is max allowed by fb
        # keep to 1, doesn't seem to slow down runtime, and also better deals with errors
        batch_size = 50
        joined_fields = ",".join(fields)
        LOGGER.info("Batch size: %d", batch_size)
        for i in range(0, len(posts), batch_size):
            try:
                post_ids_slice = list(map(lambda _post: _post["id"], posts[i: min(i + batch_size, len(posts))]))
                fields_for_posts = fb_graph.get_objects(ids=post_ids_slice, fields=joined_fields)

                for post_id in post_ids_slice:
                    fields_for_post = fields_for_posts[post_id]
                    post = {
                        "post_id": post_id,
                        "page_id": page_info["page_id"],
                        "page_name": page_info["page_name"],
                        "hashtags_count": 0,
                    }

                    queried_posts.append(post)

                    # caption, comments_count etc...
                    for metric in metrics:
                        post[renamed_columns.get(metric, metric)] = fields_for_post.get(metric)

                    mentioned_users = set()

                    for word in fields_for_post.get("caption", "").split():
                        if word[0] == "#":
                            post["hashtags_count"] += 1
                        elif word[0] == "@":
                            if word in mentioned_users:
                                continue
                            mentioned_users.add(word[1:])
                    post["mentioned_users"] = list(mentioned_users)

                    # insights
                    if "insights" in fields_for_post:
                        for insight in fields_for_post["insights"]["data"]:
                            column_name = insight["name"]
                            post[renamed_columns.get(column_name, column_name)] = insight["values"][0]["value"]

            except Exception as e:
                stack_trace = traceback.format_exc()
                LOGGER.warning(stack_trace)
                LOGGER.warning("Exception: %s", str(e))

    return queried_posts


# Syncs
def sync_page_insights(fb_graph, config, state, stream):
    mdata = metadata.to_map(stream.metadata)
    schema = stream.schema.to_dict()

    singer.write_schema(
        stream_name=stream.tap_stream_id,
        schema=schema,
        key_properties=stream.key_properties,
    )

    metrics = [
        "follower_count",
        "profile_views",
        "impressions",
        "reach",
        "email_contacts",
        "phone_call_clicks",
        "text_message_clicks",
        "get_directions_clicks",
        "website_clicks",
    ]

    bookmark_column = get_bookmark(stream.tap_stream_id)
    start_date = singer.get_bookmark(state, stream.tap_stream_id, bookmark_column) \
        if state.get("bookmarks", {}).get(stream.tap_stream_id) \
        else config["start_date"]

    start_date = get_verified_date_to_poll(start_date, stream.tap_stream_id)
    global_bookmark = start_date
    for business_account_id in config["fb_object_ids"]:
        local_bookmark = start_date
        while True:
            end_date = get_end_date(local_bookmark)
            records = query_page_insights(fb_graph, metrics, business_account_id,
                                          start_date=local_bookmark,
                                          end_date=end_date)
            with singer.metrics.record_counter(stream.tap_stream_id) as counter:
                for row in records:
                    # Type Conversation and Transformation
                    transformed_data = transform(row, schema, metadata=mdata)

                    singer.write_records(stream.tap_stream_id, [transformed_data])
                    counter.increment()
                    if bookmark_column:
                        local_bookmark = max([local_bookmark, string_to_date(transformed_data[bookmark_column])])
                if end_date >= date_today():
                    break
                local_bookmark = end_date
        global_bookmark = max([local_bookmark, global_bookmark])

    if bookmark_column:
        state = singer.write_bookmark(state, stream.tap_stream_id, bookmark_column, str(global_bookmark))
        singer.write_state(state)


def sync_page_metrics(fb_graph, stream, config):
    mdata = metadata.to_map(stream.metadata)
    schema = stream.schema.to_dict()

    singer.write_schema(
        stream_name=stream.tap_stream_id,
        schema=schema,
        key_properties=stream.key_properties,
    )
    for business_account_id in config["fb_object_ids"]:
        page_info = query_page_info(fb_graph, business_account_id)
        with singer.metrics.record_counter(stream.tap_stream_id) as counter:
            for row in [page_info]:
                # Type Conversation and Transformation
                transformed_data = transform(row, schema, metadata=mdata)

                singer.write_records(stream.tap_stream_id, [transformed_data])
                counter.increment()


def sync_posts_insights(fb_graph, config, state, stream):
    mdata = metadata.to_map(stream.metadata)
    schema = stream.schema.to_dict()

    singer.write_schema(
        stream_name=stream.tap_stream_id,
        schema=schema,
        key_properties=stream.key_properties,
    )

    bookmark_column = get_bookmark(stream.tap_stream_id)
    start_date = singer.get_bookmark(state, stream.tap_stream_id, bookmark_column) \
        if state.get("bookmarks", {}).get(stream.tap_stream_id) \
        else config["start_date"]

    start_date = get_verified_date_to_poll(start_date, stream.tap_stream_id)
    global_bookmark = start_date
    for business_account_id in config["fb_object_ids"]:
        page_info = query_page_info(fb_graph, business_account_id)

        local_bookmark = start_date
        all_posts = {"stories": [], "media": []}

        while True:
            end_date = get_end_date(local_bookmark)
            records = query_posts_insights(fb_graph, business_account_id,
                                           start_date=local_bookmark,
                                           end_date=end_date,
                                           page_info=page_info,
                                           all_posts=all_posts)
            with singer.metrics.record_counter(stream.tap_stream_id) as counter:
                for row in records:
                    # Type Conversation and Transformation
                    transformed_data = transform(row, schema, metadata=mdata)

                    singer.write_records(stream.tap_stream_id, [transformed_data])
                    counter.increment()
                    if bookmark_column:
                        local_bookmark = max([local_bookmark, string_to_date(transformed_data[bookmark_column])])
                if end_date >= date_today():
                    break
                local_bookmark = end_date
        global_bookmark = max([local_bookmark, global_bookmark])

    if bookmark_column:
        state = singer.write_bookmark(state, stream.tap_stream_id, bookmark_column, str(global_bookmark))
        singer.write_state(state)


def sync(config, state, catalog):
    selected_streams = catalog.get_selected_streams(state)
    if not selected_streams:
        return

    fb_graph = facebook.GraphAPI(access_token=config["access_token"], version=config.get("fb_api_version", 3.1))

    # Loop over selected streams in catalog
    for stream in selected_streams:
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)
        if stream.tap_stream_id in ["page_insights"]:
            sync_page_insights(fb_graph, config, state, stream)
        elif stream.tap_stream_id in ["page_overall_metrics"]:
            sync_page_metrics(fb_graph, stream, config)
        elif stream.tap_stream_id in ["posts_insights"]:
            sync_posts_insights(fb_graph, config, state, stream)


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        catalog = discover()
        catalog.dump()
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
