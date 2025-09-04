use moderation::core::rules::{evaluate_policy, EnforcementAction};

#[test]
fn evaluate_policy_thresholds() {
	let d = evaluate_policy(0.95);
	assert!(matches!(d.action, Some(EnforcementAction::Suspend)));
	let d = evaluate_policy(0.75);
	assert!(matches!(d.action, Some(EnforcementAction::Mute)));
	let d = evaluate_policy(0.55);
	assert!(matches!(d.action, Some(EnforcementAction::Warn)));
	let d = evaluate_policy(0.1);
	assert!(d.action.is_none());
}