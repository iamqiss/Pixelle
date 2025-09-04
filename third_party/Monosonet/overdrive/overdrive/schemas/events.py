from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime

class InteractionEvent(BaseModel):
	user_id: str
	note_id: str
	event_type: str
	feed_context: str = "for-you"
	timestamp: datetime
	metadata: Dict[str, Any] = Field(default_factory=dict)

class ExposureEvent(BaseModel):
	user_id: str
	note_id: str
	position: int
	seen_start: datetime
	seen_end: Optional[datetime] = None
	session_id: Optional[str] = None
	metadata: Dict[str, Any] = Field(default_factory=dict)