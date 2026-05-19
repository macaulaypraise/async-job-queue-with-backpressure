from functools import lru_cache
from typing import Annotated

from fastapi import Depends

from app.config import Settings, get_settings

# Type alias — used in route signatures for clean injection
SettingsDep = Annotated[Settings, Depends(get_settings)]
