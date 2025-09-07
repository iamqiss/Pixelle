// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// Authentication module

pub mod token;
pub mod jwt_auth;

// Re-export commonly used types
pub use token::{
    AuthManager, AuthContext, User, AccessKey, KeyStatus, 
    PolicyDocument, PolicyStatement, SignatureV4
};
pub use jwt_auth::{JwtAuthManager, NimbuxUser, UserRole, Permission, JwtConfig, AuthResult, TokenValidationResult};