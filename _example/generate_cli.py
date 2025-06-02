from _connect import _connect as _connect_
from _util import _util_helper as _util_helper_

from _util import _util_helper as _util_helper_
@_util_helper_.convert_flag(write_flg=True, output_filepath="redshift_history_load_select_stmt.py")
def get_redshift_history_load_select_stmt(database_name: str, table_name: str, output_filepath: str) -> bool:
    from _util import _util_file as _util_file_
    redshift_obj = _connect_.get_directive("redshift", "config_prod")
    response = redshift_obj.get_select_from_create_stmt(database_name=database_name,
                                             table_name=table_name
                                             )
    _util_file_.identity_write_file(output_filepath, response)
    return True