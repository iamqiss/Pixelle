use reqwest::Client;
use serde_json::{json, Value};
use std::time::Duration;
use chrono::{Utc, Datelike};

/// Comprehensive compliance and data export example for the Sonet Moderation Service
/// This demonstrates all the compliance features needed for legal compliance,
/// law enforcement requests, and quarterly business reporting.

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔒 Sonet Moderation Service - Compliance & Data Export Example");
    println!("=============================================================\n");

    // Configuration
    let base_url = "http://localhost:8080";
    let client = Client::builder()
        .timeout(Duration::from_secs(60))
        .build()?;

    // 1. Data Export for Legal Compliance
    println!("1. Data Export for Legal Compliance");
    println!("   ---------------------------------");

    // Export all user data for GDPR compliance
    let gdpr_export = create_export_request(&client, base_url, &json!({
        "requester_id": "legal_team",
        "requester_type": "legal_counsel",
        "export_scope": {
            "scope_type": "all",
            "custom_filters": {
                "data_type": "user_data",
                "jurisdiction": "EU",
                "compliance": "GDPR"
            }
        },
        "format": "json",
        "legal_basis": "GDPR Article 20 - Right to data portability",
        "urgency": "normal",
        "notes": "Export for user data portability request",
        "include_metadata": true,
        "include_evidence": false,
        "include_classifications": true,
        "include_signals": false,
        "include_reports": false,
        "include_audit_logs": true
    })).await?;
    
    println!("   📋 GDPR Export Request:");
    println!("      ID: {}", gdpr_export["data"]["id"]);
    println!("      Status: {}", gdpr_export["data"]["status"]);
    println!("      Legal Basis: GDPR Article 20");
    println!();

    // Export specific user data for legal proceedings
    let user_export = create_export_request(&client, base_url, &json!({
        "requester_id": "legal_team",
        "requester_type": "legal_counsel",
        "export_scope": {
            "scope_type": "user",
            "user_id": "user_123"
        },
        "format": "pdf",
        "legal_basis": "Court order for legal proceedings",
        "urgency": "high",
        "notes": "Export for legal case #2024-001",
        "include_metadata": true,
        "include_evidence": true,
        "include_classifications": true,
        "include_signals": true,
        "include_reports": true,
        "include_audit_logs": true
    })).await?;
    
    println!("   👤 User Data Export:");
    println!("      ID: {}", user_export["data"]["id"]);
    println!("      Status: {}", user_export["data"]["status"]);
    println!("      Format: PDF");
    println!();

    // Export content by date range for regulatory compliance
    let regulatory_export = create_export_request(&client, base_url, &json!({
        "requester_id": "compliance_team",
        "requester_type": "regulatory_body",
        "export_scope": {
            "scope_type": "date_range",
            "start_date": "2024-01-01T00:00:00Z",
            "end_date": "2024-03-31T23:59:59Z"
        },
        "format": "excel",
        "legal_basis": "Regulatory compliance reporting requirement",
        "urgency": "normal",
        "notes": "Q1 2024 regulatory compliance report",
        "include_metadata": true,
        "include_evidence": false,
        "include_classifications": true,
        "include_signals": true,
        "include_reports": true,
        "include_audit_logs": false
    })).await?;
    
    println!("   📊 Regulatory Export:");
    println!("      ID: {}", regulatory_export["data"]["id"]);
    println!("      Status: {}", regulatory_export["data"]["status"]);
    println!("      Period: Q1 2024");
    println!();

    // 2. Law Enforcement Requests
    println!("2. Law Enforcement Requests");
    println!("   -------------------------");

    // Emergency disclosure for CSAM (Child Sexual Abuse Material)
    let csam_request = create_law_enforcement_request(&client, base_url, &json!({
        "agency": "FBI Cyber Crimes Unit",
        "officer_name": "Detective Sarah Johnson",
        "badge_number": "FBI-2024-001",
        "case_number": "CSAM-2024-001",
        "legal_basis": "Emergency disclosure for child safety",
        "request_type": "emergency_disclosure",
        "urgency": "critical",
        "target_user": "user_456",
        "target_content": "content_789",
        "start_date": "2024-01-01T00:00:00Z",
        "end_date": "2024-03-31T23:59:59Z",
        "specific_evidence": [
            "Suspicious image uploads",
            "Multiple user reports",
            "AI detection alerts"
        ],
        "legal_documents": [
            "https://example.com/warrant.pdf",
            "https://example.com/emergency_order.pdf"
        ]
    })).await?;
    
    println!("   🚨 CSAM Emergency Request:");
    println!("      ID: {}", csam_request["data"]["id"]);
    println!("      Agency: {}", csam_request["data"]["agency"]);
    println!("      Case: {}", csam_request["data"]["case_number"]);
    println!("      Urgency: {}", csam_request["data"]["urgency"]);
    println!();

    // Regular law enforcement request for harassment investigation
    let harassment_request = create_law_enforcement_request(&client, base_url, &json!({
        "agency": "Local Police Department",
        "officer_name": "Officer Mike Rodriguez",
        "badge_number": "LPD-2024-002",
        "case_number": "HAR-2024-001",
        "legal_basis": "Criminal investigation - harassment",
        "request_type": "user_data",
        "urgency": "high",
        "target_user": "user_789",
        "target_content": null,
        "start_date": "2024-02-01T00:00:00Z",
        "end_date": "2024-03-15T23:59:59Z",
        "specific_evidence": [
            "User reports of harassment",
            "Threatening messages",
            "Pattern of abusive behavior"
        ],
        "legal_documents": [
            "https://example.com/subpoena.pdf"
        ]
    })).await?;
    
    println!("   👮 Harassment Investigation:");
    println!("      ID: {}", harassment_request["data"]["id"]);
    println!("      Agency: {}", harassment_request["data"]["agency"]);
    println!("      Case: {}", harassment_request["data"]["case_number"]);
    println!();

    // 3. Compliance Reports
    println!("3. Compliance Reports");
    println!("   -------------------");

    // Generate quarterly compliance report
    let quarterly_report = generate_compliance_report(&client, base_url, &json!({
        "report_type": "quarterly",
        "period_start": "2024-01-01T00:00:00Z",
        "period_end": "2024-03-31T23:59:59Z",
        "generated_by": "compliance_system"
    })).await?;
    
    println!("   📈 Quarterly Report:");
    println!("      ID: {}", quarterly_report["data"]["id"]);
    println!("      Period: {} to {}", 
        quarterly_report["data"]["period_start"], 
        quarterly_report["data"]["period_end"]);
    println!("      Status: {}", quarterly_report["data"]["compliance_status"]);
    println!("      Violations: {}", quarterly_report["data"]["total_violations"]);
    println!("      Reports: {}", quarterly_report["data"]["total_reports"]);
    println!();

    // Generate annual compliance report
    let annual_report = generate_compliance_report(&client, base_url, &json!({
        "report_type": "annual",
        "period_start": "2023-01-01T00:00:00Z",
        "period_end": "2023-12-31T23:59:59Z",
        "generated_by": "compliance_system"
    })).await?;
    
    println!("   📊 Annual Report:");
    println!("      ID: {}", annual_report["data"]["id"]);
    println!("      Period: {} to {}", 
        annual_report["data"]["period_start"], 
        annual_report["data"]["period_end"]);
    println!("      Status: {}", annual_report["data"]["compliance_status"]);
    println!();

    // 4. Export Compliance Reports
    println!("4. Export Compliance Reports");
    println!("   -------------------------");

    // Export quarterly report in Excel format
    let quarterly_export = export_compliance_report(&client, base_url, &json!({
        "report_id": quarterly_report["data"]["id"],
        "format": "excel"
    })).await?;
    
    println!("   📊 Quarterly Report Export:");
    println!("      ID: {}", quarterly_export["data"]["id"]);
    println!("      Format: Excel");
    println!("      File Size: {} bytes", quarterly_export["data"]["file_size"]);
    println!("      Records: {}", quarterly_export["data"]["record_count"]);
    println!("      Download URL: {}", quarterly_export["data"]["download_url"].as_str().unwrap_or("N/A"));
    println!();

    // Export annual report in PDF format
    let annual_export = export_compliance_report(&client, base_url, &json!({
        "report_id": annual_report["data"]["id"],
        "format": "pdf"
    })).await?;
    
    println!("   📄 Annual Report Export:");
    println!("      ID: {}", annual_export["data"]["id"]);
    println!("      Format: PDF");
    println!("      File Size: {} bytes", annual_export["data"]["file_size"]);
    println!("      Records: {}", annual_export["data"]["record_count"]);
    println!();

    // 5. Audit Logs and Compliance Tracking
    println!("5. Audit Logs and Compliance Tracking");
    println!("   -----------------------------------");

    // Get audit logs for compliance officer
    let audit_logs = get_audit_logs(&client, base_url, &json!({
        "user_id": "compliance_officer",
        "start_date": "2024-01-01T00:00:00Z",
        "end_date": "2024-03-31T23:59:59Z",
        "limit": 100
    })).await?;
    
    println!("   📝 Compliance Officer Audit Logs:");
    println!("      Total Logs: {}", audit_logs["data"].as_array().unwrap().len());
    for log in audit_logs["data"].as_array().unwrap().iter().take(3) {
        println!("      - {}: {} -> {}", 
            log["timestamp"], 
            log["action"], 
            log["outcome"]);
    }
    println!();

    // Export audit logs for legal review
    let audit_export = export_audit_logs(&client, base_url, &json!({
        "start_date": "2024-01-01T00:00:00Z",
        "end_date": "2024-03-31T23:59:59Z",
        "format": "csv",
        "include_metadata": true,
        "filters": {
            "action_types": ["export_request", "law_enforcement_request", "compliance_report"],
            "user_roles": ["compliance_officer", "legal_counsel", "admin"]
        }
    })).await?;
    
    println!("   📋 Audit Logs Export:");
    println!("      Status: {}", audit_export["success"]);
    println!();

    // 6. Emergency Data Disclosure
    println!("6. Emergency Data Disclosure");
    println!("   --------------------------");

    // Emergency disclosure for terrorism threat
    let emergency_disclosure = emergency_data_disclosure(&client, base_url, &json!({
        "threat_type": "terrorism",
        "urgency": "critical",
        "legal_basis": "Emergency disclosure for public safety",
        "requesting_agency": "FBI Counterterrorism",
        "case_number": "TERROR-2024-001",
        "target_users": ["user_123", "user_456"],
        "evidence": [
            "Threatening messages",
            "Suspicious activity patterns",
            "Multiple user reports"
        ],
        "immediate_action_required": true
    })).await?;
    
    println!("   🚨 Emergency Disclosure:");
    println!("      Status: {}", emergency_disclosure["data"]["status"]);
    println!("      Priority: {}", emergency_disclosure["data"]["priority"]);
    println!("      Response Time: {}", emergency_disclosure["data"]["response_time"]);
    println!("      Data Provided: {}", emergency_disclosure["data"]["data_provided"]);
    println!();

    // 7. Quarterly Business Reports
    println!("7. Quarterly Business Reports");
    println!("   ---------------------------");

    // Generate current quarter report
    let current_quarter = get_current_quarter();
    let quarterly_business_report = generate_quarterly_report(&client, base_url, &json!({
        "quarter": current_quarter,
        "year": 2024,
        "include_metrics": true,
        "include_analysis": true,
        "include_recommendations": true
    })).await?;
    
    println!("   📊 Quarterly Business Report:");
    println!("      Quarter: {}", current_quarter);
    println!("      Year: 2024");
    println!("      Status: Generated");
    println!();

    // Export quarterly business report
    let quarterly_business_export = export_quarterly_report(&client, base_url, &json!({
        "period": format!("Q{}_2024", current_quarter),
        "format": "excel"
    })).await?;
    
    println!("   📈 Quarterly Business Export:");
    println!("      Period: Q{} 2024", current_quarter);
    println!("      Format: Excel");
    println!("      Status: {}", quarterly_business_export["success"]);
    println!();

    // 8. Process Export Requests
    println!("8. Process Export Requests");
    println!("   -----------------------");

    // Process the GDPR export request
    let gdpr_export_id = gdpr_export["data"]["id"].as_str().unwrap();
    let gdpr_result = process_export_request(&client, base_url, gdpr_export_id).await?;
    
    println!("   📋 GDPR Export Processed:");
    println!("      Export ID: {}", gdpr_result["data"]["id"]);
    println!("      File Size: {} bytes", gdpr_result["data"]["file_size"]);
    println!("      Records: {}", gdpr_result["data"]["record_count"]);
    println!("      Checksum: {}", gdpr_result["data"]["checksum"]);
    println!("      Download URL: {}", gdpr_result["data"]["download_url"].as_str().unwrap_or("N/A"));
    println!();

    // Process the user data export
    let user_export_id = user_export["data"]["id"].as_str().unwrap();
    let user_result = process_export_request(&client, base_url, user_export_id).await?;
    
    println!("   👤 User Data Export Processed:");
    println!("      Export ID: {}", user_result["data"]["id"]);
    println!("      File Size: {} bytes", user_result["data"]["file_size"]);
    println!("      Records: {}", user_result["data"]["record_count"]);
    println!();

    // 9. Download Exports
    println!("9. Download Exports");
    println!("   -----------------");

    // Download GDPR export
    let gdpr_download = download_export(&client, base_url, gdpr_export_id).await?;
    println!("   📥 GDPR Export Download:");
    println!("      Status: {}", if gdpr_download.status().is_success() { "Success" } else { "Failed" });
    println!("      Content Type: {}", gdpr_download.headers().get("content-type").unwrap_or(&"N/A".parse().unwrap()));
    println!();

    // Download user data export
    let user_download = download_export(&client, base_url, user_export_id).await?;
    println!("   📥 User Data Export Download:");
    println!("      Status: {}", if user_download.status().is_success() { "Success" } else { "Failed" });
    println!();

    // 10. Compliance Summary
    println!("10. Compliance Summary");
    println!("    -------------------");

    // Get all export requests
    let all_exports = get_export_requests(&client, base_url).await?;
    println!("   📊 Export Requests Summary:");
    println!("      Total Requests: {}", all_exports["data"].as_array().unwrap().len());
    
    let pending = all_exports["data"].as_array().unwrap().iter()
        .filter(|e| e["status"] == "Pending")
        .count();
    let completed = all_exports["data"].as_array().unwrap().iter()
        .filter(|e| e["status"] == "Completed")
        .count();
    
    println!("      Pending: {}", pending);
    println!("      Completed: {}", completed);
    println!();

    // Get all law enforcement requests
    let all_ler = get_law_enforcement_requests(&client, base_url).await?;
    println!("   👮 Law Enforcement Requests:");
    println!("      Total Requests: {}", all_ler["data"].as_array().unwrap().len());
    
    let critical = all_ler["data"].as_array().unwrap().iter()
        .filter(|r| r["urgency"] == "Critical")
        .count();
    let high = all_ler["data"].as_array().unwrap().iter()
        .filter(|r| r["urgency"] == "High")
        .count();
    
    println!("      Critical: {}", critical);
    println!("      High: {}", high);
    println!();

    // Get all compliance reports
    let all_reports = get_compliance_reports(&client, base_url).await?;
    println!("   📈 Compliance Reports:");
    println!("      Total Reports: {}", all_reports["data"].as_array().unwrap().len());
    
    let quarterly_count = all_reports["data"].as_array().unwrap().iter()
        .filter(|r| r["report_type"] == "Quarterly")
        .count();
    let annual_count = all_reports["data"].as_array().unwrap().iter()
        .filter(|r| r["report_type"] == "Annual")
        .count();
    
    println!("      Quarterly: {}", quarterly_count);
    println!("      Annual: {}", annual_count);
    println!();

    println!("\n🎉 Compliance and data export example completed successfully!");
    println!("   The moderation service is fully compliant and ready for:");
    println!("   - Legal compliance (GDPR, CCPA, etc.)");
    println!("   - Law enforcement requests (CSAM, terrorism, etc.)");
    println!("   - Regulatory reporting");
    println!("   - Quarterly business reports");
    println!("   - Audit trails and compliance tracking");
    
    Ok(())
}

// Helper functions

fn get_current_quarter() -> u32 {
    let month = Utc::now().month();
    ((month - 1) / 3) + 1
}

async fn create_export_request(client: &Client, base_url: &str, data: &Value) -> Result<Value, Box<dyn std::error::Error>> {
    let response = client.post(&format!("{}/api/v1/compliance/exports", base_url))
        .json(data)
        .send()
        .await?;
    
    if response.status().is_success() {
        let result: Value = response.json().await?;
        Ok(result)
    } else {
        let error_text = response.text().await?;
        Err(format!("Create export request failed: {}", error_text).into())
    }
}

async fn create_law_enforcement_request(client: &Client, base_url: &str, data: &Value) -> Result<Value, Box<dyn std::error::Error>> {
    let response = client.post(&format!("{}/api/v1/compliance/law-enforcement", base_url))
        .json(data)
        .send()
        .await?;
    
    if response.status().is_success() {
        let result: Value = response.json().await?;
        Ok(result)
    } else {
        let error_text = response.text().await?;
        Err(format!("Create law enforcement request failed: {}", error_text).into())
    }
}

async fn generate_compliance_report(client: &Client, base_url: &str, data: &Value) -> Result<Value, Box<dyn std::error::Error>> {
    let response = client.post(&format!("{}/api/v1/compliance/reports", base_url))
        .json(data)
        .send()
        .await?;
    
    if response.status().is_success() {
        let result: Value = response.json().await?;
        Ok(result)
    } else {
        let error_text = response.text().await?;
        Err(format!("Generate compliance report failed: {}", error_text).into())
    }
}

async fn export_compliance_report(client: &Client, base_url: &str, data: &Value) -> Result<Value, Box<dyn std::error::Error>> {
    let response = client.post(&format!("{}/api/v1/compliance/reports/{}/export", base_url, data["report_id"]))
        .json(data)
        .send()
        .await?;
    
    if response.status().is_success() {
        let result: Value = response.json().await?;
        Ok(result)
    } else {
        let error_text = response.text().await?;
        Err(format!("Export compliance report failed: {}", error_text).into())
    }
}

async fn get_audit_logs(client: &Client, base_url: &str, data: &Value) -> Result<Value, Box<dyn std::error::Error>> {
    let mut url = format!("{}/api/v1/compliance/audit-logs", base_url);
    
    // Build query parameters
    let mut params = Vec::new();
    if let Some(user_id) = data.get("user_id") {
        params.push(format!("user_id={}", user_id));
    }
    if let Some(start_date) = data.get("start_date") {
        params.push(format!("start_date={}", start_date));
    }
    if let Some(end_date) = data.get("end_date") {
        params.push(format!("end_date={}", end_date));
    }
    if let Some(limit) = data.get("limit") {
        params.push(format!("limit={}", limit));
    }
    
    if !params.is_empty() {
        url.push('?');
        url.push_str(&params.join("&"));
    }
    
    let response = client.get(&url).send().await?;
    
    if response.status().is_success() {
        let result: Value = response.json().await?;
        Ok(result)
    } else {
        let error_text = response.text().await?;
        Err(format!("Get audit logs failed: {}", error_text).into())
    }
}

async fn export_audit_logs(client: &Client, base_url: &str, data: &Value) -> Result<Value, Box<dyn std::error::Error>> {
    let response = client.post(&format!("{}/api/v1/compliance/audit-logs/export", base_url))
        .json(data)
        .send()
        .await?;
    
    if response.status().is_success() {
        let result: Value = response.json().await?;
        Ok(result)
    } else {
        let error_text = response.text().await?;
        Err(format!("Export audit logs failed: {}", error_text).into())
    }
}

async fn emergency_data_disclosure(client: &Client, base_url: &str, data: &Value) -> Result<Value, Box<dyn std::error::Error>> {
    let response = client.post(&format!("{}/api/v1/compliance/emergency-disclosure", base_url))
        .json(data)
        .send()
        .await?;
    
    if response.status().is_success() {
        let result: Value = response.json().await?;
        Ok(result)
    } else {
        let error_text = response.text().await?;
        Err(format!("Emergency data disclosure failed: {}", error_text).into())
    }
}

async fn generate_quarterly_report(client: &Client, base_url: &str, data: &Value) -> Result<Value, Box<dyn std::error::Error>> {
    let response = client.post(&format!("{}/api/v1/compliance/quarterly-report", base_url))
        .json(data)
        .send()
        .await?;
    
    if response.status().is_success() {
        let result: Value = response.json().await?;
        Ok(result)
    } else {
        let error_text = response.text().await?;
        Err(format!("Generate quarterly report failed: {}", error_text).into())
    }
}

async fn export_quarterly_report(client: &Client, base_url: &str, data: &Value) -> Result<Value, Box<dyn std::error::Error>> {
    let period = data["period"].as_str().unwrap();
    let response = client.post(&format!("{}/api/v1/compliance/quarterly-report/{}/export", base_url, period))
        .json(data)
        .send()
        .await?;
    
    if response.status().is_success() {
        let result: Value = response.json().await?;
        Ok(result)
    } else {
        let error_text = response.text().await?;
        Err(format!("Export quarterly report failed: {}", error_text).into())
    }
}

async fn process_export_request(client: &Client, base_url: &str, export_id: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let response = client.post(&format!("{}/api/v1/compliance/exports/{}/process", base_url, export_id))
        .send()
        .await?;
    
    if response.status().is_success() {
        let result: Value = response.json().await?;
        Ok(result)
    } else {
        let error_text = response.text().await?;
        Err(format!("Process export request failed: {}", error_text).into())
    }
}

async fn download_export(client: &Client, base_url: &str, export_id: &str) -> Result<reqwest::Response, Box<dyn std::error::Error>> {
    let response = client.get(&format!("{}/api/v1/compliance/exports/{}/download", base_url, export_id))
        .send()
        .await?;
    
    Ok(response)
}

async fn get_export_requests(client: &Client, base_url: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let response = client.get(&format!("{}/api/v1/compliance/exports", base_url))
        .send()
        .await?;
    
    if response.status().is_success() {
        let result: Value = response.json().await?;
        Ok(result)
    } else {
        let error_text = response.text().await?;
        Err(format!("Get export requests failed: {}", error_text).into())
    }
}

async fn get_law_enforcement_requests(client: &Client, base_url: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let response = client.get(&format!("{}/api/v1/compliance/law-enforcement", base_url))
        .send()
        .await?;
    
    if response.status().is_success() {
        let result: Value = response.json().await?;
        Ok(result)
    } else {
        let error_text = response.text().await?;
        Err(format!("Get law enforcement requests failed: {}", error_text).into())
    }
}

async fn get_compliance_reports(client: &Client, base_url: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let response = client.get(&format!("{}/api/v1/compliance/reports", base_url))
        .send()
        .await?;
    
    if response.status().is_success() {
        let result: Value = response.json().await?;
        Ok(result)
    } else {
        let error_text = response.text().await?;
        Err(format!("Get compliance reports failed: {}", error_text).into())
    }
}