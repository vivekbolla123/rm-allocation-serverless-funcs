def get_input_file_upload():
    return "SELECT table_name, role, user_name, init_timestamp FROM inputs_file_upload WHERE obj_id = :obj_id"

def update_upload_time_taken():
    return "UPDATE inputs_file_upload SET status = :status, done_timestamp = :done_timestamp, total_rows = :total_rows, total_time_taken = :total_time_taken WHERE obj_id = :obj_id"

def input_file_upload_error_occurred():
    return "UPDATE inputs_file_upload SET errors = :errors WHERE obj_id = :obj_id"

def status_in_progress():
    return "UPDATE inputs_file_upload SET status = :status, inprogress_timestamp = :inprogress_timestamp WHERE obj_id = :obj_id"

def get_column_headers_query():
    return "SELECT column_names, id_column_names FROM config_column_names WHERE table_name = :table_name"

def copy_values_table_to_table(table_name, temp_table_name):
    return f"INSERT INTO {table_name} SELECT * FROM {temp_table_name}"

def delete_all_values_query(table_name):
    return f"DELETE FROM {table_name}"

def delete_key_values_query(table_name, temp_table_name, id_columns):
    return f"""
        DELETE FROM {table_name}
        WHERE ({id_columns}) IN (
            SELECT {id_columns} FROM {temp_table_name}
        );
    """

def insert_values_query(table_name, column_names, placeholders):
    return f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"

def GET_CURRENT_VERSION_COUNT():
    return """
        SELECT count(*) AS count
        FROM current_table_version
        WHERE table_name = :table_name
    """


def UPDATE_CURRENT_VERSION():
    return """
        UPDATE current_table_version
        SET curr_version = :uuid
        WHERE table_name = :table_name
    """


def INSERT_CURRENT_VERSION():
    return """
        INSERT INTO current_table_version (curr_version, table_name)
        VALUES (:uuid, :table_name)
    """

def DELETE_TABLE(table_name):
    return f"DROP TABLE IF EXISTS {table_name}"

