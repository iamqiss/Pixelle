use moderation::core::classifier::{ContentClassifier, RuleBasedClassifier, ClassificationLabel};

#[test]
fn rule_based_classifier_basic() {
	let c = RuleBasedClassifier;
	let r = c.classify("This is clean text");
	assert!(matches!(r.label, ClassificationLabel::Clean));
	let r = c.classify("BUY NOW free $$$");
	assert!(matches!(r.label, ClassificationLabel::Spam));
}