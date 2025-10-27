import os
from typing import Dict, List
import pandas as pd
import numpy as np

CAT_COLS: List[str] = ["merch", "cat_id", "gender", "one_city", "us_state", "jobs"]
NUM_COLS: List[str] = [
    "amount", "lat", "lon", "population_city",
    "merchant_lat", "merchant_lon", "hour", "dayofweek"
]
DROP_COLS: List[str] = ["transaction_time", "name_1", "name_2", "street", "post_code"]

_DEFAULT_ORDER = CAT_COLS + NUM_COLS
_FEATURES_FILE = os.getenv("FEATURES_PATH", "./models/features.txt")

def _load_features_order() -> List[str]:
    if os.path.isfile(_FEATURES_FILE):
        try:
            with open(_FEATURES_FILE, "r", encoding="utf-8") as f:
                lines = [ln.strip() for ln in f.readlines() if ln.strip()]
            filtered = [c for c in lines if c in (CAT_COLS + NUM_COLS)]
            return filtered if filtered else _DEFAULT_ORDER
        except Exception:
            return _DEFAULT_ORDER
    return _DEFAULT_ORDER

FEATURES_ORDER: List[str] = _load_features_order()

def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:

    for c in CAT_COLS:
        if c not in df.columns:
            df[c] = "unknown"
    for c in NUM_COLS:
        if c not in df.columns:
            df[c] = 0

    return df

def preprocess_one(record: Dict) -> pd.DataFrame:

    df = pd.DataFrame([record]).copy()

    if "transaction_time" in df.columns:
        ts = pd.to_datetime(df["transaction_time"], errors="coerce")
        df["hour"] = ts.dt.hour
        df["dayofweek"] = ts.dt.dayofweek

    drop_present = [c for c in DROP_COLS if c in df.columns]
    if drop_present:
        df.drop(columns=drop_present, inplace=True, errors="ignore")

    df = _ensure_columns(df)

    for c in CAT_COLS:
        df[c] = df[c].astype(str).fillna("unknown")

    for c in NUM_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce")
        fill_val = 0 if df[c].isna().all() else df[c].median()
        df[c] = df[c].fillna(fill_val)

    for col in FEATURES_ORDER:
        if col not in df.columns:
            df[col] = "unknown" if col in CAT_COLS else 0
    X = df[FEATURES_ORDER]

    return X
