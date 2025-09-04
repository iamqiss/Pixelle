use serde::{Deserialize, Serialize};

use crate::core::rules::EnforcementAction;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionOutcome {
	pub success: bool,
	pub message: String,
}

pub async fn execute(action: EnforcementAction, user_id: &str) -> ActionOutcome {
	let message = match action {
		EnforcementAction::Warn => format!("warned user {}", user_id),
		EnforcementAction::Mute => format!("muted user {}", user_id),
		EnforcementAction::Suspend => format!("suspended user {}", user_id),
	};
	ActionOutcome { success: true, message }
}