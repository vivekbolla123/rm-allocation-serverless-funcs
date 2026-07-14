import json
import pandas as pd

from common.utils.db_helper import DbHelper
from common.utils.logger import logger
from common.constants.constants import SINGLE_RBD_MAP_PARAMETER_KEY

db_helper = DbHelper()


def _load_single_rbd_map() -> dict:
    """
    Load SINGLE_RBD_MAP from the rm_parameter_values collection.
    The stored value maps expanded -> single (e.g. {"QA": "Q", "O0": "O", ...}).
    We invert it to single -> expanded for the formatting step.
    """
    raw = db_helper.get_parameter_value(SINGLE_RBD_MAP_PARAMETER_KEY)
    if raw is None:
        logger.error("SINGLE_RBD_MAP parameter not found in DB. Cannot format saver file.")
        return {}
    try:
        stored_map = json.loads(raw)
        return {v: k for k, v in stored_map.items()}
    except (json.JSONDecodeError, TypeError) as e:
        logger.error(f"Failed to parse SINGLE_RBD_MAP parameter: {e}")
        return {}


def format_saver_file(df: pd.DataFrame) -> pd.DataFrame:
    single_rbd_map = _load_single_rbd_map()
    if not single_rbd_map:
        raise ValueError("SINGLE_RBD_MAP could not be loaded from the database.")

    df = df.copy()
    if 'rbd' not in df.columns:
        raise KeyError("Column 'rbd' not found in DataFrame.")

    isna = df['rbd'].isna()
    s = df['rbd'].astype(str)

    mask_single = s.str.len() == 1
    mapped = s.where(~mask_single, s.map(single_rbd_map)).fillna(s)

    mapped = mapped.mask(isna)

    df['rbd'] = mapped

    return df
