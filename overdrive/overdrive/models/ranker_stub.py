from typing import List, Dict, Any

class RankerStub:
	def rank_for_you(self, user_id: str, candidate_ids: List[str], features: Dict[str, Any], limit: int = 20) -> List[Dict[str, Any]]:
		# TODO: call into C++ serving via gRPC; here we return placeholder
		scored = [{"note_id": cid, "score": 0.0, "factors": {}, "reasons": []} for cid in candidate_ids]
		return scored[:limit]