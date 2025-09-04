package xyz.sonet.app.ui.settings

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.layout.ContentScale
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import androidx.lifecycle.viewmodel.compose.viewModel
import xyz.sonet.app.viewmodels.SecurityViewModel
import xyz.sonet.app.models.SecuritySession
import xyz.sonet.app.models.SecurityLog
import xyz.sonet.app.models.DeviceType
import xyz.sonet.app.models.SecurityEventType

@Composable
fun SecurityView(
    modifier: Modifier = Modifier,
    viewModel: SecurityViewModel = viewModel(),
    onNavigateBack: () -> Unit = {}
) {
    val requirePasswordForPurchases by viewModel.requirePasswordForPurchases.collectAsState()
    val requirePasswordForSettings by viewModel.requirePasswordForSettings.collectAsState()
    val biometricEnabled by viewModel.biometricEnabled.collectAsState()
    val biometricType by viewModel.biometricType.collectAsState()
    val activeSessions by viewModel.activeSessions.collectAsState()
    val securityLogs by viewModel.securityLogs.collectAsState()
    val loginAlertsEnabled by viewModel.loginAlertsEnabled.collectAsState()
    val suspiciousActivityAlertsEnabled by viewModel.suspiciousActivityAlertsEnabled.collectAsState()
    val passwordChangeAlertsEnabled by viewModel.passwordChangeAlertsEnabled.collectAsState()
    val isLoading by viewModel.isLoading.collectAsState()
    val error by viewModel.error.collectAsState()
    
    LaunchedEffect(Unit) {
        viewModel.loadSecuritySettings()
        viewModel.loadActiveSessions()
        viewModel.loadSecurityLogs()
    }
    
    Scaffold(
        topBar = {
            TopAppBar(
                title = { Text("Security") },
                navigationIcon = {
                    IconButton(onClick = onNavigateBack) {
                        Icon(Icons.Default.ArrowBack, contentDescription = "Back")
                    }
                }
            )
        }
    ) { paddingValues ->
        LazyColumn(
            modifier = modifier
                .fillMaxSize()
                .padding(paddingValues),
            contentPadding = PaddingValues(vertical = 8.dp)
        ) {
            // Password Section
            item {
                SecuritySection(
                    title = "Password",
                    subtitle = "Configure when your password is required for sensitive actions."
                ) {
                    SecurityItem(
                        icon = Icons.Default.Lock,
                        title = "Change Password",
                        onClick = { /* Navigate to change password */ }
                    )
                    
                    SecurityItem(
                        icon = Icons.Default.History,
                        title = "Password History",
                        onClick = { /* Navigate to password history */ }
                    )
                    
                    SecurityToggle(
                        title = "Require Password for App Store",
                        checked = requirePasswordForPurchases,
                        onCheckedChange = { checked ->
                            viewModel.updatePasswordRequirement(PasswordRequirementAction.PURCHASES, checked)
                        }
                    )
                    
                    SecurityToggle(
                        title = "Require Password for Settings",
                        checked = requirePasswordForSettings,
                        onCheckedChange = { checked ->
                            viewModel.updatePasswordRequirement(PasswordRequirementAction.SETTINGS, checked)
                        }
                    )
                }
            }
            
            // Biometric Authentication Section
            item {
                SecuritySection(
                    title = "Biometric Authentication",
                    subtitle = "Use your fingerprint or face to quickly access the app."
                ) {
                    if (biometricType != DeviceType.NONE) {
                        SecurityToggle(
                            title = "Use ${biometricType.displayName}",
                            checked = biometricEnabled,
                            onCheckedChange = { checked ->
                                viewModel.updateBiometricAuthentication(checked)
                            }
                        )
                        
                        if (biometricEnabled) {
                            Text(
                                text = "You can use ${biometricType.displayName} to unlock the app and authorize sensitive actions.",
                                style = MaterialTheme.typography.bodySmall,
                                color = MaterialTheme.colorScheme.onSurfaceVariant,
                                modifier = Modifier.padding(horizontal = 16.dp, vertical = 8.dp)
                            )
                        }
                    } else {
                        Text(
                            text = "Biometric authentication is not available on this device.",
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                            modifier = Modifier.padding(horizontal = 16.dp, vertical = 8.dp)
                        )
                    }
                }
            }
            
            // Active Sessions Section
            item {
                SecuritySection(
                    title = "Active Sessions",
                    subtitle = "Manage devices and browsers that are currently signed into your account."
                ) {
                    if (activeSessions.isEmpty) {
                        Text(
                            text = "No active sessions",
                            style = MaterialTheme.typography.bodyMedium,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                            modifier = Modifier.padding(horizontal = 16.dp, vertical = 8.dp)
                        )
                    } else {
                        activeSessions.forEach { session ->
                            SessionRow(
                                session = session,
                                isCurrentSession = session.id == viewModel.currentSessionId,
                                onRevoke = {
                                    viewModel.revokeSession(session.id)
                                }
                            )
                        }
                        
                        TextButton(
                            onClick = { viewModel.loadAllSessions() },
                            modifier = Modifier.padding(horizontal = 16.dp)
                        ) {
                            Text("View All Sessions")
                        }
                    }
                }
            }
            
            // Security Logs Section
            item {
                SecuritySection(
                    title = "Recent Security Events",
                    subtitle = "Track important security events like logins, password changes, and suspicious activity."
                ) {
                    if (securityLogs.isEmpty) {
                        Text(
                            text = "No security events",
                            style = MaterialTheme.typography.bodyMedium,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                            modifier = Modifier.padding(horizontal = 16.dp, vertical = 8.dp)
                        )
                    } else {
                        securityLogs.take(5).forEach { log ->
                            SecurityLogRow(log = log)
                        }
                        
                        if (securityLogs.size > 5) {
                            TextButton(
                                onClick = { /* Navigate to all security logs */ },
                                modifier = Modifier.padding(horizontal = 16.dp)
                            ) {
                                Text("View All Security Events (${securityLogs.size})")
                            }
                        }
                    }
                }
            }
            
            // Security Alerts Section
            item {
                SecuritySection(
                    title = "Security Alerts",
                    subtitle = "Get notified about important security events on your account."
                ) {
                    SecurityToggle(
                        title = "Login Alerts",
                        checked = loginAlertsEnabled,
                        onCheckedChange = { checked ->
                            viewModel.updateLoginAlerts(checked)
                        }
                    )
                    
                    SecurityToggle(
                        title = "Suspicious Activity Alerts",
                        checked = suspiciousActivityAlertsEnabled,
                        onCheckedChange = { checked ->
                            viewModel.updateSuspiciousActivityAlerts(checked)
                        }
                    )
                    
                    SecurityToggle(
                        title = "Password Change Alerts",
                        checked = passwordChangeAlertsEnabled,
                        onCheckedChange = { checked ->
                            viewModel.updatePasswordChangeAlerts(checked)
                        }
                    )
                }
            }
            
            // Advanced Security Section
            item {
                SecuritySection(
                    title = "Advanced Security"
                ) {
                    SecurityItem(
                        icon = Icons.Default.Devices,
                        title = "Trusted Devices",
                        onClick = { /* Navigate to trusted devices */ }
                    )
                    
                    SecurityItem(
                        icon = Icons.Default.History,
                        title = "Login History",
                        onClick = { /* Navigate to login history */ }
                    )
                    
                    SecurityItem(
                        icon = Icons.Default.QuestionAnswer,
                        title = "Security Questions",
                        onClick = { /* Navigate to security questions */ }
                    )
                }
            }
        }
        
        // Error Snackbar
        error?.let { errorMessage ->
            LaunchedEffect(errorMessage) {
                // Show error snackbar
            }
        }
    }
}

// MARK: - Security Section
@Composable
fun SecuritySection(
    title: String,
    subtitle: String? = null,
    content: @Composable () -> Unit
) {
    Card(
        modifier = Modifier
            .fillMaxWidth()
            .padding(horizontal = 16.dp, vertical = 8.dp),
        colors = CardDefaults.cardColors(
            containerColor = MaterialTheme.colorScheme.surface
        ),
        elevation = CardDefaults.cardElevation(defaultElevation = 2.dp)
    ) {
        Column(
            modifier = Modifier.padding(16.dp)
        ) {
            Text(
                text = title,
                style = MaterialTheme.typography.titleMedium,
                fontWeight = FontWeight.Bold,
                color = MaterialTheme.colorScheme.onSurface
            )
            
            subtitle?.let {
                Spacer(modifier = Modifier.height(4.dp))
                Text(
                    text = it,
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
            
            Spacer(modifier = Modifier.height(16.dp))
            content()
        }
    }
}

// MARK: - Security Item
@Composable
fun SecurityItem(
    icon: ImageVector,
    title: String,
    onClick: () -> Unit
) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .clickable { onClick() }
            .padding(vertical = 12.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        Icon(
            imageVector = icon,
            contentDescription = null,
            tint = MaterialTheme.colorScheme.primary,
            modifier = Modifier.size(24.dp)
        )
        
        Spacer(modifier = Modifier.width(16.dp))
        
        Text(
            text = title,
            style = MaterialTheme.typography.bodyMedium,
            modifier = Modifier.weight(1f)
        )
        
        Icon(
            imageVector = Icons.Default.ChevronRight,
            contentDescription = null,
            tint = MaterialTheme.colorScheme.onSurfaceVariant
        )
    }
}

// MARK: - Security Toggle
@Composable
fun SecurityToggle(
    title: String,
    checked: Boolean,
    onCheckedChange: (Boolean) -> Unit
) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .padding(vertical = 12.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        Text(
            text = title,
            style = MaterialTheme.typography.bodyMedium,
            modifier = Modifier.weight(1f)
        )
        
        Switch(
            checked = checked,
            onCheckedChange = onCheckedChange
        )
    }
}

// MARK: - Session Row
@Composable
fun SessionRow(
    session: SecuritySession,
    isCurrentSession: Boolean,
    onRevoke: () -> Unit
) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .padding(vertical = 8.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        Icon(
            imageVector = session.deviceType.icon,
            contentDescription = null,
            tint = MaterialTheme.colorScheme.primary,
            modifier = Modifier.size(24.dp)
        )
        
        Spacer(modifier = Modifier.width(12.dp))
        
        Column(
            modifier = Modifier.weight(1f)
        ) {
            Row(
                verticalAlignment = Alignment.CenterVertically
            ) {
                Text(
                    text = session.deviceName,
                    style = MaterialTheme.typography.bodyMedium,
                    fontWeight = FontWeight.Medium
                )
                
                if (isCurrentSession) {
                    Spacer(modifier = Modifier.width(8.dp))
                    Text(
                        text = "(Current)",
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.primary
                    )
                }
            }
            
            Text(
                text = session.location,
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
            
            Text(
                text = "Last active: ${session.getTimeAgoDisplay()}",
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
        
        if (!isCurrentSession) {
            TextButton(
                onClick = onRevoke
            ) {
                Text("Revoke")
            }
        }
    }
}

// MARK: - Security Log Row
@Composable
fun SecurityLogRow(
    log: SecurityLog
) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .padding(vertical = 8.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        Icon(
            imageVector = log.eventType.icon,
            contentDescription = null,
            tint = log.eventType.color,
            modifier = Modifier.size(24.dp)
        )
        
        Spacer(modifier = Modifier.width(12.dp))
        
        Column(
            modifier = Modifier.weight(1f)
        ) {
            Text(
                text = log.eventType.displayName,
                style = MaterialTheme.typography.bodyMedium,
                fontWeight = FontWeight.Medium
            )
            
            Text(
                text = log.description,
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
            
            Text(
                text = log.getTimeAgoDisplay(),
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
        
        if (log.requiresAttention) {
            Icon(
                imageVector = Icons.Default.Warning,
                contentDescription = null,
                tint = MaterialTheme.colorScheme.secondary
            )
        }
    }
}

// MARK: - Supporting Types
enum class PasswordRequirementAction(val displayName: String) {
    PURCHASES("App Store purchases"),
    SETTINGS("Settings changes")
}

enum class DeviceType(val displayName: String, val icon: ImageVector) {
    MOBILE("Mobile", Icons.Default.Phone),
    TABLET("Tablet", Icons.Default.Tablet),
    DESKTOP("Desktop", Icons.Default.Computer),
    WEB("Web", Icons.Default.Language),
    NONE("None", Icons.Default.DeviceUnknown)
}

enum class SecurityEventType(
    val displayName: String,
    val icon: ImageVector,
    val color: Color,
    val requiresAttention: Boolean = false
) {
    LOGIN("Login", Icons.Default.PersonAdd, Color(0xFF6B6B6B)),
    LOGOUT("Logout", Icons.Default.PersonRemove, Color(0xFF6B6B6B)),
    PASSWORD_CHANGED("Password Changed", Icons.Default.Key, Color(0xFF6B6B6B)),
    PASSWORD_REQUIREMENT_CHANGED("Password Requirement Changed", Icons.Default.KeyOff, Color(0xFF6B6B6B)),
    BIOMETRIC_ENABLED("Biometric Enabled", Icons.Default.Fingerprint, Color(0xFF6B6B6B)),
    BIOMETRIC_DISABLED("Biometric Disabled", Icons.Default.FingerprintOff, Color(0xFF6B6B6B)),
    SESSION_REVOKED("Session Revoked", Icons.Default.Block, Color(0xFF9E9E9E), true),
    SUSPICIOUS_ACTIVITY("Suspicious Activity", Icons.Default.Warning, Color(0xFF9E9E9E), true),
    TWO_FACTOR_ENABLED("2FA Enabled", Icons.Default.Security, Color(0xFF6B6B6B)),
    TWO_FACTOR_DISABLED("2FA Disabled", Icons.Default.SecurityOff, Color(0xFF6B6B6B))
}