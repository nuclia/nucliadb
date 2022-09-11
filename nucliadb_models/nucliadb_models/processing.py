from typing import Optional

from pydantic import BaseModel


class PushProcessingOptions(BaseModel):
    # Enable ML processing
    ml_text: Optional[bool] = True
