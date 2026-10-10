import logging.handlers
import os
import time
import logging
from threading import Thread
import pymongo
from pymongo.errors import ServerSelectionTimeoutError
from dotenv import load_dotenv
import json

from init_sync import init_sync
from listening import listening
from schema_utils import init_table_schema
from constants import (
    METADATA_FILE_NAME,
    PARTNER_EVENTS_FILE_NAME
)
from push_file_to_lz import push_file_to_lz
from file_utils import FileType, read_from_file
from utils import run_with_retry

# How often (in seconds) to re-scan MongoDB for newly created collections when
# MONGO_COLLECTION=all. Can be overridden via the NEW_COLLECTION_SCAN_INTERVAL_SEC
# environment variable.
DEFAULT_NEW_COLLECTION_SCAN_INTERVAL_SEC = 600

def mirror():
    load_dotenv()
    log_format_os = os.getenv("APP_LOG_LEVEL")
    print(f"log_level before getlevels =={log_format_os}")
    # changed to _nameToLevel as getLevelNamesMapping is available from python 3.11
    #log_level = logging.getLevelNamesMapping().get(log_format_os, logging.INFO)
    log_level = logging._nameToLevel.get(log_format_os, logging.INFO)
    #Display Log level set
    print(f"log_level set ={log_level}")
    log_format_str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=log_level, format=log_format_str)
    root_logger = logging.getLogger()
    logging_formatter = logging.Formatter(log_format_str)
    #Changed to rotate logs
    #file_handler = logging.FileHandler("mirroring.log")
    file_handler = logging.handlers.RotatingFileHandler('mirroring.log', maxBytes=50*1024*1024, backupCount=5)

    file_handler.setFormatter(logging_formatter)
    root_logger.addHandler(file_handler)

    logger = logging.getLogger(__name__)
    if (
        not os.getenv("MONGO_CONN_STR")
        or not os.getenv("MONGO_DB_NAME")
        or not os.getenv("MONGO_COLLECTION")
        or not os.getenv("LZ_URL")
        or not os.getenv("APP_ID")
        or not os.getenv("SECRET")
        or not os.getenv("TENANT_ID")
        or not os.getenv("INIT_LOAD_BATCH_SIZE")
        or not os.getenv("DELTA_SYNC_BATCH_SIZE")
    # added threshold time    
        or not os.getenv("TIME_THRESHOLD_IN_SEC")
    ):
        raise ValueError("Missing environment variable.")

    mongodb_coll_name = os.getenv("MONGO_COLLECTION")
    collection_list = []
    all_collections = __get_all_collections()
    if mongodb_coll_name == "all":
        collection_list = all_collections
    elif mongodb_coll_name.startswith("["):
        collection_list = json.loads(mongodb_coll_name)
    elif isinstance(mongodb_coll_name, str):
        collection_list = [mongodb_coll_name]
    else:
        raise ValueError(
            'Invalid parameter value: mongodb_coll_name. "\
            "Expected a list of collection names, a str of a single collection"\
            " name, or "all" for all collections in the database.'
        )

    # threads: list[Thread] = []

    # remove non-exists collections
    removed_collections = []
    collection_list = [
        item
        for item in collection_list
        if item in all_collections or removed_collections.append(item) is None
    ]
    for non_exists_collection in removed_collections:
        logger.warning(f"removed non-exists collection {non_exists_collection}")

    for collection_name in collection_list:
        __start_mirroring_for_collection(collection_name, logger)

    # When tracking "all" collections, periodically re-scan the database so
    # collections created after this process started are picked up without
    # requiring a restart (previously collection_list was computed once at
    # startup and never revisited).
    if mongodb_coll_name == "all":
        Thread(
            target=__rescan_for_new_collections,
            args=(set(collection_list), logger),
            daemon=True,
        ).start()

    # for thread in threads:
    #     thread.join()
    while True:
        cmd = input()
        if cmd.lower() == "quit":
            os._exit(0)


def __start_mirroring_for_collection(collection_name: str, logger: logging.Logger) -> bool:
    """
    Perform one-time setup for a collection (metadata/partner-events files,
    schema init) and start its listening thread. Isolated in its own
    try/except so a failure for one collection (e.g. a transient LZ/auth
    error) only skips that collection instead of aborting the caller and
    silently preventing other collections from ever starting.

    Returns True if the collection's listening thread was started.
    """
    try:
#>>># changes to write metadata.json a the first file - 6Mar2025
        metadata_file_exists = read_from_file(
            collection_name, METADATA_FILE_NAME, FileType.TEXT
        )
        if not metadata_file_exists:
            metadata_json_path = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)), METADATA_FILE_NAME
                )
            logger.info("writing metadata file to LZ")
            push_file_to_lz(metadata_json_path, collection_name)

        partner_events_file_exists = read_from_file(
            collection_name, PARTNER_EVENTS_FILE_NAME, FileType.TEXT
        )
        if not partner_events_file_exists:
            partner_events_template_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "_partnerEvents_template.json"
            )
            partner_events_output_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), PARTNER_EVENTS_FILE_NAME
            )
            logger.info("writing _partnerEvents.json file to LZ")
            with open(partner_events_template_path, 'r') as f:
                partner_events_content = f.read()
            partner_events_content = partner_events_content \
                .replace('${MONGO_DB_NAME}', os.getenv('MONGO_DB_NAME', '')) \
                .replace('${MONGO_COLLECTION}', collection_name) \
                .replace('${LZ_URL}', os.getenv('LZ_URL', ''))
            with open(partner_events_output_path, 'w') as output_file:
                output_file.write(partner_events_content)
            push_file_to_lz(partner_events_output_path, collection_name)

        init_table_schema(collection_name)

        # listening() persists its progress (resume tokens) to file, so it
        # is safe to transparently restart it with backoff if it ever
        # crashes from an unexpected/unhandled exception.
        Thread(
            target=run_with_retry,
            args=(listening,),
            kwargs={"args": (collection_name,), "name": f"listening[{collection_name}]"},
        ).start()
        ## ---------- RainCity changes ---------- 
        version = "2026-07-28_1106"
        logger.info(f"\n\n\n\n*********** App version: {version} ***********\n\n\n")
        ## ---------- End of RainCity changes ----------

        # listener_thread = Thread(target=listening, args=(collection_name,))
        # listener_thread.start()
        # threads.append(listener_thread)

        # Moved the starting of init_sync to listening so as not to miss any records which may come by the time we start init_sync
        # Thread(target=init_sync, args=(collection_name,)).start()
        # init_thread = Thread(target=init_sync, args=(collection_name,))
        # init_thread.start()
        # threads.append(init_thread)
        return True
    except Exception:
        logger.error(
            f"Failed to set up collection {collection_name}; skipping it for now.",
            exc_info=True,
        )
        return False


def __rescan_for_new_collections(known_collections: set, logger: logging.Logger):
    """
    Periodically re-list the database's collections and start mirroring any
    that weren't present (or weren't yet created) when this process started.
    Only used when MONGO_COLLECTION=all.
    """
    scan_interval_sec = int(
        os.getenv("NEW_COLLECTION_SCAN_INTERVAL_SEC", DEFAULT_NEW_COLLECTION_SCAN_INTERVAL_SEC)
    )
    while True:
        time.sleep(scan_interval_sec)
        try:
            current_collections = __get_all_collections()
        except Exception:
            logger.error("Failed to re-scan collections; will retry next interval.", exc_info=True)
            continue

        new_collections = [c for c in current_collections if c not in known_collections]
        for collection_name in new_collections:
            logger.info(f"discovered new collection {collection_name}; starting mirroring for it")
            if __start_mirroring_for_collection(collection_name, logger):
                known_collections.add(collection_name)


def __get_all_collections() -> list[str]:
    client = pymongo.MongoClient(os.getenv("MONGO_CONN_STR"))
    # check database existence
    db_name = os.getenv("MONGO_DB_NAME")
    print(f"db_name={db_name}")
    try:
        all_db_names = client.list_database_names()
        if db_name not in all_db_names:
            raise ValueError(f"Database name provided do not exists: {db_name}")
        db = client[db_name]
        return db.list_collection_names()
    except ServerSelectionTimeoutError:
        raise ValueError("Can not connect to MongoDB with the provided MONGO_CONN_STR.")


if __name__ == "__main__":
    mirror()
