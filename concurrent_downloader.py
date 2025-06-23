"""
Concurrent processing of dockets to download comments.

This cell implements asynchronous processing to download comments for multiple dockets 
concurrently. It uses a pool of API keys as workers and divides the list of docket IDs 
into chunks (jobs). Each worker processes a chunk of dockets, saving comments to a temporary SQLite database.
Finally, all temporary databases are merged into a single, combined SQLite database.
"""
import nest_asyncio
import asyncio
import os
import sqlite3
import re
from comments_downloader import CommentsDownloader 
import logging

nest_asyncio.apply()

def setup_worker_logger(worker_id: int, log_dir: str = "logs"):
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(f"worker_{worker_id}")
    logger.setLevel(logging.INFO)

    # Avoid adding multiple handlers if re-running cells
    if not logger.handlers:
        fh = logging.FileHandler(os.path.join(log_dir, f"worker_{worker_id}.log"))
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    
    return logger

# --- Configuration for Concurrent Processing ---
FINAL_DB_NAME = "data/EPA_Comments_Combined.db"  # Name of the final combined database
TABLES_TO_MERGE = [
    "comments_detail",
    "comments_header",
    "dockets_detail",
    "dockets_header",
    "documents_detail",
    "documents_header"
]
# Max number of concurrent tasks making network requests. 
# Actual concurrency will also be limited by the number of available API keys.
MAX_CONCURRENT_WORKERS = 200
# Number of dockets a single worker task will process. Adjust for balance.
DOCKETS_PER_TASK_CHUNK = 1 
TEMP_DB_DIR = "data/temp_dbs" # Directory to store temporary databases

# --- Helper function to merge SQLite databases ---
def merge_databases(temp_db_files, final_db_path, tables_to_merge):
    print(f"\nStarting merge of {len(temp_db_files)} temporary databases into {final_db_path}...")
    os.makedirs(os.path.dirname(final_db_path), exist_ok=True)

    final_conn = sqlite3.connect(final_db_path)
    final_cursor = final_conn.cursor()

    first_valid_temp_db = next((f for f in temp_db_files if f and os.path.exists(f) and os.path.getsize(f) > 0), None)

    if not first_valid_temp_db:
        print("No valid temporary databases found to infer schemas. Aborting merge.")
        final_conn.close()
        return

    for table_name in tables_to_merge:
        created_table_in_final = False
        try:
            temp_conn_s = sqlite3.connect(first_valid_temp_db)
            temp_cursor_s = temp_conn_s.cursor()
            temp_cursor_s.execute(
                f"SELECT sql FROM sqlite_master WHERE type='table' AND lower(name)='{table_name.lower()}'"
            )
            schema_row = temp_cursor_s.fetchone()
            if schema_row:
                create_table_sql = schema_row[0]
                pattern = re.compile(r"CREATE\s+TABLE\s+", re.IGNORECASE)
                if not re.search(r"IF\s+NOT\s+EXISTS", create_table_sql, re.IGNORECASE):
                    match = re.search(
                        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([`\"]?\w+[`\"]?)",
                        create_table_sql,
                        re.IGNORECASE,
                    )
                    if match:
                        tn = match.group(1)
                        create_table_sql = create_table_sql.replace(
                            f"CREATE TABLE {tn}", f"CREATE TABLE IF NOT EXISTS {tn}", 1
                        )
                    else:
                        create_table_sql = pattern.sub(
                            f"CREATE TABLE IF NOT EXISTS {table_name} ", create_table_sql, 1
                        )
                final_cursor.execute(create_table_sql)
                final_conn.commit()
                created_table_in_final = True
                print(f"Ensured table '{table_name}' exists in {final_db_path} using schema from {first_valid_temp_db}.")
            else:
                print(f"Warning: Table '{table_name}' not found in schema source {first_valid_temp_db}.")
            temp_conn_s.close()
        except sqlite3.Error as e:
            print(f"Error preparing table '{table_name}' in final DB using schema from {first_valid_temp_db}: {e}")

        if not created_table_in_final:
            print(f"CRITICAL: Table '{table_name}' could not be created in {final_db_path}.")

    total_rows_merged_all_tables = 0
    for table_name in tables_to_merge:
        print(f"\n--- Merging data for table: {table_name} ---")
        current_table_rows_merged = 0
        for temp_db_path in temp_db_files:
            if not temp_db_path or not os.path.exists(temp_db_path) or os.path.getsize(temp_db_path) == 0:
                continue
            try:
                temp_conn = sqlite3.connect(temp_db_path)
                temp_cursor = temp_conn.cursor()
                temp_cursor.execute(
                    f"SELECT name FROM sqlite_master WHERE type='table' AND lower(name)='{table_name.lower()}'"
                )
                if not temp_cursor.fetchone():
                    temp_conn.close()
                    continue
                temp_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count_in_temp = temp_cursor.fetchone()[0]
                if row_count_in_temp == 0:
                    temp_conn.close()
                    continue
                temp_cursor.execute(f"SELECT * FROM {table_name}")
                for row in temp_cursor:
                    try:
                        placeholders = ', '.join(['?'] * len(row))
                        final_cursor.execute(
                            f"INSERT OR IGNORE INTO {table_name} VALUES ({placeholders})", row
                        )
                        if final_cursor.rowcount > 0:
                            current_table_rows_merged += 1
                    except sqlite3.Error:
                        pass
                final_conn.commit()
                temp_conn.close()
            except sqlite3.Error as e:
                print(f"Error processing table '{table_name}' in {temp_db_path}: {e}.")
        print(f"Total {current_table_rows_merged} new rows merged into table '{table_name}'.")
        total_rows_merged_all_tables += current_table_rows_merged

    print("\nCleaning up temporary database files...")
    for temp_db_path in temp_db_files:
        if temp_db_path and os.path.exists(temp_db_path):
            try:
                os.remove(temp_db_path)
            except OSError as e:
                print(f"Error deleting temp file {temp_db_path}: {e}")
    final_conn.close()
    print(f"\nMerge complete. Total {total_rows_merged_all_tables} new rows merged across all tables into {final_db_path}.")

    try:
        final_conn = sqlite3.connect(final_db_path)
        final_cursor = final_conn.cursor()
        print("\nFinal number of comments:")
        for table_name in ['comments_detail']:
            try:
                final_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = final_cursor.fetchone()[0]
                print(f"  {table_name}: {count} row(s)")
            except sqlite3.Error as e:
                print(f"  Error counting rows in table '{table_name}': {e}")
        final_conn.close()
    except Exception as e:
        print(f"Error opening final database for row count summary: {e}")


# --- Asynchronous Worker Function ---
from asyncio import Lock
total_dockets_processed = 0
counter_lock = Lock()

async def process_docket_chunk(api_key_queue: asyncio.Queue, docket_chunk: list, worker_id: int, temp_dir: str):
    """
    Processes a chunk of dockets using an API key from the queue.
    Downloads comments and saves them to a temporary database.
    Uses asyncio.to_thread for blocking operations within CommentsDownloader.
    """
    logger = setup_worker_logger(worker_id)

    api_key = await api_key_queue.get()  # Get an API key from the queue
    temp_db_filename = os.path.join(temp_dir, f"temp_worker_comments_{worker_id}.db")
    temp_csv_filename = os.path.join(temp_dir, f"temp_worker_comments_{worker_id}.csv")
    logger.info(f"Acquired API key ...{api_key[-4:]}. Processing {len(docket_chunk)} dockets.")
    global total_dockets_processed

    try:
        # Initialize CommentsDownloader with the specific API key
        downloader = CommentsDownloader(api_key=api_key)
        
        for docket_id in docket_chunk:
            logger.info(f"Processing docket: {docket_id}")
            try:
                logger.info(f"PINEAPPLE: {docket_id}")
                # Run the (assumed) blocking gather_comments_by_docket in a separate thread
                await asyncio.to_thread(
                    downloader.gather_comments_by_docket,
                    docket_id,
                    db_filename=temp_db_filename,
                    csv_filename=None # We want data in DB for merging
                )
                logger.info(f"Successfully processed docket: {docket_id}")
                
                async with counter_lock:
                    print("BANANA")
                    total_dockets_processed += 1

            except Exception as e:
                logger.error(f"Error processing docket {docket_id}: {e}")
                
                async with counter_lock:
                    print("BLUEBERRY")
                    total_dockets_processed += 1
                # Optional: Implement retry logic or add docket_id to a failed queue
        
        return temp_db_filename # Return path to the temporary DB for merging
    except Exception as e:
        logger.error(f"Major error: {e}")
        return None # Indicate failure for this worker task
    finally:
        await api_key_queue.put(api_key) # Return the API key to the queue
        # api_key_queue.task_done() # Not strictly needed if not using queue.join()
        logger.info("Released API key and finished processing.")

# --- Main Orchestration for Concurrent Downloading ---
async def run_concurrent_downloading_main(all_docket_ids: list, all_api_keys: list):
    """
    Orchestrates the concurrent downloading of comments for all dockets.
    Uses 'all_api_keys' (from Cell 1 or 2, e.g., 'loaded_api_keys') and 
    'all_docket_ids' (from Cell 4, e.g., 'docket_ids').
    """
    print("--- Starting Concurrent Docket Processing ---")
    if not all_api_keys:
        print("Error: No API keys available (e.g., 'loaded_api_keys' is empty). Cannot proceed.")
        return
    if not all_docket_ids:
        print("Error: No docket IDs to process (e.g., 'docket_ids' is empty). Cannot proceed.")
        return

    # Create a temporary directory for worker databases if it doesn't exist
    os.makedirs(TEMP_DB_DIR, exist_ok=True)

    # Create an asyncio.Queue for managing API keys
    api_key_queue = asyncio.Queue()
    for key in all_api_keys:
        await api_key_queue.put(key)
    
    # Determine the number of concurrent worker tasks
    # Limited by MAX_CONCURRENT_WORKERS and the number of available API keys.
    num_active_workers = min(MAX_CONCURRENT_WORKERS, len(all_api_keys))
    print(f"Configured to use up to {num_active_workers} concurrent workers.")

    # Divide dockets into chunks for tasks
    all_docket_ids = list(dict.fromkeys(all_docket_ids))
    num_total_dockets = len(all_docket_ids)
    chunk_size = DOCKETS_PER_TASK_CHUNK
    docket_chunks = [all_docket_ids[i:i + chunk_size] for i in range(0, num_total_dockets, chunk_size)]
    print(f"Divided {num_total_dockets} dockets into {len(docket_chunks)} chunks of up to {chunk_size} dockets each.")

    # Create and launch tasks
    # A semaphore can also be used here to explicitly limit active asyncio.to_thread calls if needed,
    # beyond the API key queue limit, but the queue itself acts as a resource limiter.
    tasks = []
    for i, chunk in enumerate(docket_chunks):
        task = asyncio.create_task(process_docket_chunk(api_key_queue, chunk, i, TEMP_DB_DIR))
        tasks.append(task)

    # Wait for all tasks to complete and collect results (paths to temp DBs)
    print(f"\nLaunching {len(tasks)} processing tasks...")
    temp_db_files_results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Filter out None results (from workers that failed) and exceptions
    valid_temp_db_files = []
    for i, result in enumerate(temp_db_files_results):
        if isinstance(result, Exception):
            print(f"Task for chunk {i} raised an exception: {result}")
        elif result is None:
            print(f"Task for chunk {i} (worker) failed to produce a database file.")
        else:
            valid_temp_db_files.append(result)
            
    print(f"\nAll tasks completed. {len(valid_temp_db_files)} temporary database(s) were successfully created.")

    # Merge temporary databases into the final one
    if valid_temp_db_files:
        merge_databases(valid_temp_db_files, FINAL_DB_NAME, TABLES_TO_MERGE)
    else:
        print("No temporary databases were successfully created. Nothing to merge.")

    # Attempt to clean up temp_dir if empty, otherwise, list remaining files for manual inspection
    try:
        if os.path.exists(TEMP_DB_DIR) and not os.listdir(TEMP_DB_DIR):
            os.rmdir(TEMP_DB_DIR)
            print(f"Successfully removed empty temporary directory: {TEMP_DB_DIR}")
        elif os.path.exists(TEMP_DB_DIR):
            print(f"Warning: Temporary directory {TEMP_DB_DIR} is not empty. It contains: {os.listdir(TEMP_DB_DIR)}. Manual cleanup might be needed.")
    except OSError as e:
        print(f"Error cleaning up temp directory {TEMP_DB_DIR}: {e}")

    print(f"\nTotal dockets successfully processed: {total_dockets_processed}")
    print("--- Concurrent downloading and merging process finished. ---")

# if __name__ == '__main__': # This check is for script execution, in Jupyter just run the cell.
#     # Check if variables are defined (would be from previous cells in Jupyter)
#     if 'loaded_api_keys' in locals() and 'docket_ids' in locals():
#         if loaded_api_keys and docket_ids: # Ensure they are not empty
#             asyncio.run(run_concurrent_downloading_main(all_docket_ids=docket_ids, all_api_keys=loaded_api_keys))
#             print("To run concurrent processing: uncomment the asyncio.run(...) line above and execute this cell.")
#             print("Ensure `loaded_api_keys` and `docket_ids` are populated from previous cells.")
#         else:
#             print("API keys or docket IDs list is empty. Skipping concurrent download.")
#     else:
#         print("Run previous cells to define 'loaded_api_keys' and 'docket_ids' before executing concurrent download.")

# To run in Jupyter: uncomment the line below after ensuring loaded_api_keys and docket_ids exist.
# await run_concurrent_downloading_main(all_docket_ids=docket_ids, all_api_keys=loaded_api_keys)
# await run_concurrent_downloading_main(all_docket_ids=["EPA-R09-OAR-2009-0697"], all_api_keys=loaded_api_keys)