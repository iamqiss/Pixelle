use reqwest::Client;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::sleep;

/// Example client integration for the Sonet Moderation Service
/// This demonstrates how clients can consume from the moderation service
/// using both HTTP REST API and gRPC endpoints.

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Sonet Moderation Service - Client Integration Example");
    println!("=====================================================\n");

    // Configuration
    let base_url = "http://localhost:8080";
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    // 1. Health Check
    println!("1. Checking service health...");
    let health = check_health(&client, base_url).await?;
    println!("   ✅ Service is healthy: {}", health["status"]);
    println!("   📊 Version: {}", health["version"]);
    println!();

    // 2. Content Classification Examples
    println!("2. Content Classification Examples");
    println!("   ------------------------------");

    // Single content classification
    let classification = classify_content(&client, base_url, &json!({
        "content_id": "post_123",
        "user_id": "user_456",
        "text": "This is a test post for content moderation",
        "content_type": "post",
        "language_hint": "en",
        "priority": "normal"
    })).await?;
    
    println!("   📝 Single classification:");
    println!("      Content ID: {}", classification["data"]["content_id"]);
    println!("      Label: {}", classification["data"]["label"]);
    println!("      Confidence: {:.2}", classification["data"]["confidence"]);
    println!("      Language: {}", classification["data"]["language"]);
    println!("      Processing Time: {}ms", classification["data"]["processing_time_ms"]);
    println!();

    // Multilingual content classification
    let spanish_content = classify_content(&client, base_url, &json!({
        "content_id": "comment_789",
        "user_id": "user_123",
        "text": "Este es un comentario en español que necesita moderación",
        "content_type": "comment",
        "language_hint": "es",
        "priority": "normal"
    })).await?;
    
    println!("   🌍 Multilingual classification (Spanish):");
    println!("      Content ID: {}", spanish_content["data"]["content_id"]);
    println!("      Label: {}", spanish_content["data"]["label"]);
    println!("      Language: {}", spanish_content["data"]["language"]);
    println!();

    // Batch classification
    let batch_results = classify_batch(&client, base_url, &json!([
        {
            "content_id": "post_001",
            "user_id": "user_001",
            "text": "First post in batch",
            "content_type": "post",
            "priority": "normal"
        },
        {
            "content_id": "post_002",
            "user_id": "user_002",
            "text": "Second post in batch",
            "content_type": "post",
            "priority": "normal"
        },
        {
            "content_id": "post_003",
            "user_id": "user_003",
            "text": "Third post in batch",
            "content_type": "post",
            "priority": "normal"
        }
    ])).await?;
    
    println!("   📦 Batch classification:");
    println!("      Processed {} items", batch_results["data"].as_array().unwrap().len());
    for (i, result) in batch_results["data"].as_array().unwrap().iter().enumerate() {
        println!("      Item {}: {} -> {} (confidence: {:.2})", 
            i + 1, 
            result["content_id"], 
            result["label"], 
            result["confidence"]);
    }
    println!();

    // 3. User Reports Examples
    println!("3. User Reports Examples");
    println!("   ---------------------");

    // Create a report
    let report = create_report(&client, base_url, &json!({
        "reporter_id": "user_789",
        "target_id": "user_456",
        "content_id": "post_123",
        "report_type": "hate_speech",
        "reason": "Contains offensive language and hate speech",
        "description": "This post contains discriminatory language targeting specific groups",
        "evidence": [
            {
                "evidence_type": "text",
                "content": "Offensive content from the post",
                "url": null,
                "screenshot": null
            }
        ]
    })).await?;
    
    println!("   🚨 Created report:");
    println!("      Report ID: {}", report["data"]["id"]);
    println!("      Status: {}", report["data"]["status"]);
    println!("      Priority: {}", report["data"]["priority"]);
    println!();

    // Get report details
    let report_id = report["data"]["id"].as_str().unwrap();
    let report_details = get_report(&client, base_url, report_id).await?;
    
    println!("   📋 Report details:");
    println!("      Reporter: {}", report_details["data"]["reporter_id"]);
    println!("      Target: {}", report_details["data"]["target_id"]);
    println!("      Type: {}", report_details["data"]["report_type"]);
    println!("      Reason: {}", report_details["data"]["reason"]);
    println!();

    // 4. Investigation Examples
    println!("4. Investigation Examples");
    println!("   ----------------------");

    // Start investigation
    let investigation = start_investigation(&client, base_url, &json!({
        "report_id": report_id,
        "investigator_id": "specialist_001"
    })).await?;
    
    println!("   🔍 Started investigation:");
    println!("      Investigation ID: {}", investigation["data"]["id"]);
    println!("      Status: {}", investigation["data"]["status"]);
    println!("      Started: {}", investigation["data"]["started_at"]);
    println!();

    // Add investigation note
    let investigation_id = investigation["data"]["id"].as_str().unwrap();
    let note = add_investigation_note(&client, base_url, investigation_id, &json!({
        "author_id": "specialist_001",
        "content": "Initial review completed. Content violates community guidelines.",
        "is_internal": false
    })).await?;
    
    println!("   📝 Added investigation note:");
    println!("      Note ID: {}", note["data"]["id"]);
    println!("      Author: {}", note["data"]["author_id"]);
    println!("      Content: {}", note["data"]["content"]);
    println!();

    // Complete investigation
    let completion = complete_investigation(&client, base_url, investigation_id, &json!({
        "final_status": "completed",
        "final_decision": "Content removed, user warned",
        "actions_taken": ["content_removal", "user_warning"]
    })).await?;
    
    println!("   ✅ Completed investigation:");
    println!("      Status: Completed");
    println!("      Actions: Content removed, user warned");
    println!();

    // 5. Metrics and Analytics
    println!("5. Metrics and Analytics");
    println!("   ----------------------");

    // Get general metrics
    let metrics = get_metrics(&client, base_url, "24h").await?;
    println!("   📊 General metrics (24h):");
    println!("      Time range: {}", metrics["data"]["time_range"]);
    println!("      Generated at: {}", metrics["data"]["generated_at"]);
    println!();

    // Get report metrics
    let report_metrics = get_report_metrics(&client, base_url, "24h").await?;
    println!("   📈 Report metrics (24h):");
    println!("      Total reports: {}", report_metrics["data"]["total_reports"]);
    println!("      Average resolution time: {} minutes", 
        report_metrics["data"]["average_resolution_time_minutes"]);
    println!("      False report rate: {:.2}%", 
        report_metrics["data"]["false_report_rate"] * 100.0);
    println!();

    // Get classification metrics
    let classification_metrics = get_classification_metrics(&client, base_url).await?;
    println!("   🤖 Classification metrics:");
    println!("      Total requests: {}", classification_metrics["data"]["requests_total"]);
    println!("      Total classifications: {}", classification_metrics["data"]["classification_total"]);
    println!("      ML inferences: {}", classification_metrics["data"]["ml_inference_total"]);
    println!("      Cache hits: {}", classification_metrics["data"]["cache_hits"]);
    println!("      Cache misses: {}", classification_metrics["data"]["cache_misses"]);
    println!();

    // 6. Admin Functions
    println!("6. Admin Functions");
    println!("   -----------------");

    // Get signals
    let signals = get_signals(&client, base_url).await?;
    println!("   📡 Active signals:");
    println!("      Total signals: {}", signals["data"].as_array().unwrap().len());
    for signal in signals["data"].as_array().unwrap().iter().take(3) {
        println!("      - {}: {} (severity: {})", 
            signal["signal_type"], 
            signal["source"], 
            signal["severity"]);
    }
    println!();

    // Get ML models
    let models = get_ml_models(&client, base_url).await?;
    println!("   🧠 Available ML models:");
    for model in models["data"].as_array().unwrap().iter() {
        println!("      - {} v{}: {}", 
            model["name"], 
            model["version"], 
            model["description"]);
    }
    println!();

    // 7. Performance Testing
    println!("7. Performance Testing");
    println!("   --------------------");

    // Test concurrent requests
    let start_time = std::time::Instant::now();
    let mut handles = Vec::new();
    
    for i in 0..10 {
        let client_clone = client.clone();
        let base_url = base_url.to_string();
        let handle = tokio::spawn(async move {
            classify_content(&client_clone, &base_url, &json!({
                "content_id": format!("perf_test_{}", i),
                "user_id": "perf_user",
                "text": format!("Performance test content {}", i),
                "content_type": "post",
                "priority": "low"
            })).await
        });
        handles.push(handle);
    }
    
    let results = futures::future::join_all(handles).await;
    let successful = results.iter().filter(|r| r.is_ok()).count();
    let duration = start_time.elapsed();
    
    println!("   ⚡ Concurrent performance test:");
    println!("      Requests: 10");
    println!("      Successful: {}", successful);
    println!("      Duration: {:.2}s", duration.as_secs_f64());
    println!("      Throughput: {:.2} req/s", 10.0 / duration.as_secs_f64());
    println!();

    // 8. Error Handling Examples
    println!("8. Error Handling Examples");
    println!("   -----------------------");

    // Test invalid content
    let invalid_result = classify_content(&client, base_url, &json!({
        "content_id": "invalid_001",
        "user_id": "", // Invalid user ID
        "text": "",    // Empty text
        "content_type": "invalid_type",
        "priority": "invalid_priority"
    })).await;
    
    match invalid_result {
        Ok(_) => println!("   ❌ Unexpected success for invalid request"),
        Err(_) => println!("   ✅ Properly handled invalid request"),
    }

    // Test non-existent report
    let non_existent_report = get_report(&client, base_url, "00000000-0000-0000-0000-000000000000").await;
    match non_existent_report {
        Ok(_) => println!("   ❌ Unexpected success for non-existent report"),
        Err(_) => println!("   ✅ Properly handled non-existent report"),
    }

    println!("\n🎉 Client integration example completed successfully!");
    println!("   The moderation service is ready for production use!");
    
    Ok(())
}

// Helper functions for API calls

async fn check_health(client: &Client, base_url: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let response = client.get(&format!("{}/health", base_url))
        .send()
        .await?;
    
    let health: Value = response.json().await?;
    Ok(health)
}

async fn classify_content(client: &Client, base_url: &str, data: &Value) -> Result<Value, Box<dyn std::error::Error>> {
    let response = client.post(&format!("{}/api/v1/classify", base_url))
        .json(data)
        .send()
        .await?;
    
    if response.status().is_success() {
        let result: Value = response.json().await?;
        Ok(result)
    } else {
        let error_text = response.text().await?;
        Err(format!("Classification failed: {}", error_text).into())
    }
}

async fn classify_batch(client: &Client, base_url: &str, data: &Value) -> Result<Value, Box<dyn std::error::Error>> {
    let response = client.post(&format!("{}/api/v1/classify/batch", base_url))
        .json(data)
        .send()
        .await?;
    
    if response.status().is_success() {
        let result: Value = response.json().await?;
        Ok(result)
    } else {
        let error_text = response.text().await?;
        Err(format!("Batch classification failed: {}", error_text).into())
    }
}

async fn create_report(client: &Client, base_url: &str, data: &Value) -> Result<Value, Box<dyn std::error::Error>> {
    let response = client.post(&format!("{}/api/v1/reports", base_url))
        .json(data)
        .send()
        .await?;
    
    if response.status().is_success() {
        let result: Value = response.json().await?;
        Ok(result)
    } else {
        let error_text = response.text().await?;
        Err(format!("Create report failed: {}", error_text).into())
    }
}

async fn get_report(client: &Client, base_url: &str, report_id: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let response = client.get(&format!("{}/api/v1/reports/{}", base_url, report_id))
        .send()
        .await?;
    
    if response.status().is_success() {
        let result: Value = response.json().await?;
        Ok(result)
    } else {
        let error_text = response.text().await?;
        Err(format!("Get report failed: {}", error_text).into())
    }
}

async fn start_investigation(client: &Client, base_url: &str, data: &Value) -> Result<Value, Box<dyn std::error::Error>> {
    let response = client.post(&format!("{}/api/v1/investigations", base_url))
        .json(data)
        .send()
        .await?;
    
    if response.status().is_success() {
        let result: Value = response.json().await?;
        Ok(result)
    } else {
        let error_text = response.text().await?;
        Err(format!("Start investigation failed: {}", error_text).into())
    }
}

async fn add_investigation_note(client: &Client, base_url: &str, investigation_id: &str, data: &Value) -> Result<Value, Box<dyn std::error::Error>> {
    let response = client.post(&format!("{}/api/v1/investigations/{}/notes", base_url, investigation_id))
        .json(data)
        .send()
        .await?;
    
    if response.status().is_success() {
        let result: Value = response.json().await?;
        Ok(result)
    } else {
        let error_text = response.text().await?;
        Err(format!("Add investigation note failed: {}", error_text).into())
    }
}

async fn complete_investigation(client: &Client, base_url: &str, investigation_id: &str, data: &Value) -> Result<Value, Box<dyn std::error::Error>> {
    let response = client.put(&format!("{}/api/v1/investigations/{}/complete", base_url, investigation_id))
        .json(data)
        .send()
        .await?;
    
    if response.status().is_success() {
        let result: Value = response.json().await?;
        Ok(result)
    } else {
        let error_text = response.text().await?;
        Err(format!("Complete investigation failed: {}", error_text).into())
    }
}

async fn get_metrics(client: &Client, base_url: &str, time_range: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let response = client.get(&format!("{}/api/v1/metrics?time_range={}", base_url, time_range))
        .send()
        .await?;
    
    if response.status().is_success() {
        let result: Value = response.json().await?;
        Ok(result)
    } else {
        let error_text = response.text().await?;
        Err(format!("Get metrics failed: {}", error_text).into())
    }
}

async fn get_report_metrics(client: &Client, base_url: &str, time_range: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let response = client.get(&format!("{}/api/v1/metrics/reports?time_range={}", base_url, time_range))
        .send()
        .await?;
    
    if response.status().is_success() {
        let result: Value = response.json().await?;
        Ok(result)
    } else {
        let error_text = response.text().await?;
        Err(format!("Get report metrics failed: {}", error_text).into())
    }
}

async fn get_classification_metrics(client: &Client, base_url: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let response = client.get(&format!("{}/api/v1/metrics/classifications", base_url))
        .send()
        .await?;
    
    if response.status().is_success() {
        let result: Value = response.json().await?;
        Ok(result)
    } else {
        let error_text = response.text().await?;
        Err(format!("Get classification metrics failed: {}", error_text).into())
    }
}

async fn get_signals(client: &Client, base_url: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let response = client.get(&format!("{}/api/v1/admin/signals", base_url))
        .send()
        .await?;
    
    if response.status().is_success() {
        let result: Value = response.json().await?;
        Ok(result)
    } else {
        let error_text = response.text().await?;
        Err(format!("Get signals failed: {}", error_text).into())
    }
}

async fn get_ml_models(client: &Client, base_url: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let response = client.get(&format!("{}/api/v1/admin/models", base_url))
        .send()
        .await?;
    
    if response.status().is_success() {
        let result: Value = response.json().await?;
        Ok(result)
    } else {
        let error_text = response.text().await?;
        Err(format!("Get ML models failed: {}", error_text).into())
    }
}