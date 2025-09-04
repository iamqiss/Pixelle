use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EnforcementAction {
	Warn,
	Mute,
	Suspend,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleDecision {
	pub action: Option<EnforcementAction>,
}

pub fn evaluate_policy(score: f32) -> RuleDecision {
	if score > 0.9 {
		RuleDecision { action: Some(EnforcementAction::Suspend) }
	} else if score > 0.7 {
		RuleDecision { action: Some(EnforcementAction::Mute) }
	} else if score > 0.5 {
		RuleDecision { action: Some(EnforcementAction::Warn) }
	} else {
		RuleDecision { action: None }
	}
}