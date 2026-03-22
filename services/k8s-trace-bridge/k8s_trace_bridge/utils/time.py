import time
from datetime import datetime, timezone
from typing import Union

class TimeUtils:
    @staticmethod
    def now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def now_epoch_seconds() -> int:
        return int(time.time())
    
    @staticmethod
    def now_utc_compact() -> str:
        return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    
    @staticmethod
    def to_epoch_seconds(dt: Union[datetime, int, None]) -> int:
        if isinstance(dt, int):
            return dt
        if isinstance(dt, datetime):
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        else:
            raise ValueError("Input must be a datetime object or an integer representing epoch seconds")
