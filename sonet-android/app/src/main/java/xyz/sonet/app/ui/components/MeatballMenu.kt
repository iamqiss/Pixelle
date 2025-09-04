package xyz.sonet.app.ui.components

import androidx.compose.animation.core.*
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.BasicTextField
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.*
import androidx.compose.material.icons.outlined.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.draw.scale
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.layout.ContentScale
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import coil.compose.AsyncImage
import coil.request.ImageRequest

@Composable
fun MeatballMenu(
    isPresented: Boolean,
    onDismiss: () -> Unit
) {
    var searchText by remember { mutableStateOf("") }
    
    if (isPresented) {
        Box(modifier = Modifier.fillMaxSize()) {
            // Background overlay
            Box(
                modifier = Modifier
                    .fillMaxSize()
                    .background(Color.Black.copy(alpha = 0.3f))
                    .clickable { onDismiss() }
            )
            
            // Menu content
            Row(
                modifier = Modifier.fillMaxHeight()
            ) {
                Spacer(modifier = Modifier.weight(1f))
                
                Card(
                    modifier = Modifier
                        .width(320.dp)
                        .fillMaxHeight()
                        .padding(16.dp),
                    colors = CardDefaults.cardColors(
                        containerColor = MaterialTheme.colorScheme.surface
                    ),
                    shape = RoundedCornerShape(16.dp),
                    elevation = CardDefaults.cardElevation(defaultElevation = 8.dp)
                ) {
                    Column {
                        // Menu header
                        MenuHeader(onDismiss = onDismiss)
                        
                        // Search bar
                        SearchBar(
                            text = searchText,
                            onTextChange = { searchText = it }
                        )
                        
                        // Menu content
                        LazyColumn(
                            modifier = Modifier.weight(1f),
                            contentPadding = PaddingValues(vertical = 8.dp)
                        ) {
                            // Profile Section
                            item { ProfileSection() }
                            
                            item { 
                                Divider(
                                    modifier = Modifier.padding(horizontal = 16.dp),
                                    color = MaterialTheme.colorScheme.outlineVariant
                                )
                            }
                            
                            // Core Navigation
                            item { CoreNavigationSection() }
                            
                            item { 
                                Divider(
                                    modifier = Modifier.padding(horizontal = 16.dp),
                                    color = MaterialTheme.colorScheme.outlineVariant
                                )
                            }
                            
                            // Creation & Discovery
                            item { CreationDiscoverySection() }
                            
                            item { 
                                Divider(
                                    modifier = Modifier.padding(horizontal = 16.dp),
                                    color = MaterialTheme.colorScheme.outlineVariant
                                )
                            }
                            
                            // Settings & Preferences
                            item { SettingsPreferencesSection() }
                            
                            item { 
                                Divider(
                                    modifier = Modifier.padding(horizontal = 16.dp),
                                    color = MaterialTheme.colorScheme.outlineVariant
                                )
                            }
                            
                            // Utility & Support
                            item { UtilitySupportSection() }
                        }
                    }
                }
            }
        }
    }
}

// MARK: - Menu Header
@Composable
fun MenuHeader(onDismiss: () -> Unit) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .padding(horizontal = 20.dp, vertical = 16.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        Text(
            text = "Menu",
            style = MaterialTheme.typography.titleLarge,
            fontWeight = FontWeight.Bold,
            color = MaterialTheme.colorScheme.onSurface
        )
        
        Spacer(modifier = Modifier.weight(1f))
        
        TextButton(onClick = onDismiss) {
            Text("Done")
        }
    }
}

// MARK: - Search Bar
@Composable
fun SearchBar(
    text: String,
    onTextChange: (String) -> Unit
) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .padding(horizontal = 20.dp, vertical = 8.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        Icon(
            imageVector = Icons.Default.Search,
            contentDescription = "Search",
            tint = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.size(20.dp)
        )
        
        Spacer(modifier = Modifier.width(12.dp))
        
        BasicTextField(
            value = text,
            onValueChange = onTextChange,
            textStyle = MaterialTheme.typography.bodyLarge.copy(
                color = MaterialTheme.colorScheme.onSurface
            ),
            modifier = Modifier.weight(1f),
            decorationBox = { innerTextField ->
                if (text.isEmpty()) {
                    Text(
                        text = "Search menu",
                        style = MaterialTheme.typography.bodyLarge,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
                innerTextField()
            }
        )
        
        if (text.isNotEmpty()) {
            Spacer(modifier = Modifier.width(8.dp))
            
            TextButton(onClick = { onTextChange("") }) {
                Text("Clear")
            }
        }
    }
    .padding(horizontal = 16.dp, vertical = 12.dp)
    .background(
        MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.3f),
        RoundedCornerShape(12.dp)
    )
    .padding(horizontal = 20.dp, vertical = 8.dp)
}

// MARK: - Profile Section
@Composable
fun ProfileSection() {
    Column(
        modifier = Modifier
            .fillMaxWidth()
            .padding(horizontal = 20.dp, vertical = 16.dp),
        verticalArrangement = Arrangement.spacedBy(16.dp)
    ) {
        // Profile info
        Row(
            verticalAlignment = Alignment.CenterVertically
        ) {
            // Profile picture
            AsyncImage(
                model = ImageRequest.Builder(LocalContext.current)
                    .data("https://example.com/avatar.jpg")
                    .crossfade(true)
                    .build(),
                contentDescription = "Profile picture",
                modifier = Modifier
                    .size(60.dp)
                    .clip(CircleShape),
                contentScale = ContentScale.Crop,
                error = {
                    Box(
                        modifier = Modifier
                            .size(60.dp)
                            .clip(CircleShape)
                            .background(MaterialTheme.colorScheme.surfaceVariant),
                        contentAlignment = Alignment.Center
                    ) {
                        Icon(
                            imageVector = Icons.Default.Person,
                            contentDescription = "Profile placeholder",
                            tint = MaterialTheme.colorScheme.onSurfaceVariant
                        )
                    }
                }
            )
            
            Spacer(modifier = Modifier.width(16.dp))
            
            // Profile details
            Column {
                Text(
                    text = "John Doe",
                    style = MaterialTheme.typography.titleMedium,
                    fontWeight = FontWeight.SemiBold,
                    color = MaterialTheme.colorScheme.onSurface
                )
                
                Text(
                    text = "@johndoe",
                    style = MaterialTheme.typography.bodyMedium,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
            
            Spacer(modifier = Modifier.weight(1f))
        }
        
        // Quick links
        Row(
            horizontalArrangement = Arrangement.spacedBy(12.dp)
        ) {
            Button(
                onClick = { /* Navigate to profile */ },
                colors = ButtonDefaults.buttonColors(
                    containerColor = MaterialTheme.colorScheme.primary
                ),
                shape = RoundedCornerShape(20.dp)
            ) {
                Text("View Profile")
            }
            
            OutlinedButton(
                onClick = { /* Navigate to edit profile */ },
                shape = RoundedCornerShape(20.dp)
            ) {
                Text("Edit Profile")
            }
        }
    }
}

// MARK: - Core Navigation Section
@Composable
fun CoreNavigationSection() {
    Column {
        MenuItem(
            icon = Icons.Default.Notifications,
            iconColor = Color(0xFFFF9800), // Orange
            title = "Notifications",
            subtitle = "Manage your notifications"
        ) {
            // Navigate to notifications
        }
        
        MenuItem(
            icon = Icons.Default.Message,
            iconColor = Color(0xFF2196F3), // Blue
            title = "Messages",
            subtitle = "View your conversations"
        ) {
            // Navigate to messages
        }
        
        MenuItem(
            icon = Icons.Default.People,
            iconColor = Color(0xFF4CAF50), // Green
            title = "Friends",
            subtitle = "Manage your connections"
        ) {
            // Navigate to friends
        }
        
        MenuItem(
            icon = Icons.Default.Bookmark,
            iconColor = Color(0xFF9C27B0), // Purple
            title = "Saved Posts",
            subtitle = "Your bookmarked content"
        ) {
            // Navigate to saved posts
        }
        
        MenuItem(
            icon = Icons.Default.Folder,
            iconColor = Color(0xFF3F51B5), // Indigo
            title = "Lists",
            subtitle = "Organize your content"
        ) {
            // Navigate to lists
        }
    }
}

// MARK: - Creation & Discovery Section
@Composable
fun CreationDiscoverySection() {
    Column {
        MenuItem(
            icon = Icons.Default.AddCircle,
            iconColor = Color(0xFF4CAF50), // Green
            title = "Create a Post",
            subtitle = "Share something new"
        ) {
            // Navigate to post creation
        }
        
        MenuItem(
            icon = Icons.Default.Search,
            iconColor = Color(0xFF2196F3), // Blue
            title = "Explore",
            subtitle = "Discover new content"
        ) {
            // Navigate to explore
        }
        
        MenuItem(
            icon = Icons.Default.TrendingUp,
            iconColor = Color(0xFFF44336), // Red
            title = "Trending",
            subtitle = "See what's popular"
        ) {
            // Navigate to trending
        }
    }
}

// MARK: - Settings & Preferences Section
@Composable
fun SettingsPreferencesSection() {
    Column {
        MenuItem(
            icon = Icons.Default.Settings,
            iconColor = Color(0xFF9E9E9E), // Gray
            title = "Settings",
            subtitle = "Account, privacy, security"
        ) {
            // Navigate to settings
            // This would typically navigate to SettingsView
        }
        
        MenuItem(
            icon = Icons.Default.Palette,
            iconColor = Color(0xFF9C27B0), // Purple
            title = "Appearance",
            subtitle = "Dark/light mode toggle"
        ) {
            // Toggle appearance
        }
        
        MenuItem(
            icon = Icons.Default.Lock,
            iconColor = Color(0xFF2196F3), // Blue
            title = "Privacy & Safety",
            subtitle = "Manage your privacy"
        ) {
            // Navigate to privacy settings
        }
    }
}

// MARK: - Utility & Support Section
@Composable
fun UtilitySupportSection() {
    Column {
        MenuItem(
            icon = Icons.Default.Help,
            iconColor = Color(0xFFFF9800), // Orange
            title = "Help & Support",
            subtitle = "Get help and support"
        ) {
            // Navigate to help
        }
        
        MenuItem(
            icon = Icons.Default.Description,
            iconColor = Color(0xFF9E9E9E), // Gray
            title = "Terms & Policies",
            subtitle = "Legal information"
        ) {
            // Navigate to terms
        }
        
        MenuItem(
            icon = Icons.Default.Logout,
            iconColor = Color(0xFFF44336), // Red
            title = "Log Out",
            subtitle = "Sign out of your account"
        ) {
            // Handle logout
        }
    }
}

// MARK: - Menu Item
@Composable
fun MenuItem(
    icon: ImageVector,
    iconColor: Color,
    title: String,
    subtitle: String,
    onClick: () -> Unit
) {
    Surface(
        modifier = Modifier
            .fillMaxWidth()
            .clickable { onClick() },
        color = Color.Transparent
    ) {
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 20.dp, vertical = 16.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            // Icon
            Icon(
                imageVector = icon,
                contentDescription = title,
                tint = iconColor,
                modifier = Modifier.size(24.dp)
            )
            
            Spacer(modifier = Modifier.width(16.dp))
            
            // Content
            Column(
                modifier = Modifier.weight(1f)
            ) {
                Text(
                    text = title,
                    style = MaterialTheme.typography.bodyLarge,
                    fontWeight = FontWeight.Medium,
                    color = MaterialTheme.colorScheme.onSurface
                )
                
                Text(
                    text = subtitle,
                    style = MaterialTheme.typography.bodyMedium,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
            
            Spacer(modifier = Modifier.width(8.dp))
            
            // Chevron
            Icon(
                imageVector = Icons.Default.ChevronRight,
                contentDescription = "Navigate",
                tint = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.size(20.dp)
            )
        }
    }
}

// MARK: - Meatball Menu Button
@Composable
fun MeatballMenuButton(
    onClick: () -> Unit
) {
    IconButton(
        onClick = onClick,
        modifier = Modifier
            .size(40.dp)
            .background(
                MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.3f),
                RoundedCornerShape(8.dp)
            )
    ) {
        Column(
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.spacedBy(2.dp)
        ) {
            repeat(3) {
                Box(
                    modifier = Modifier
                        .size(4.dp)
                        .clip(CircleShape)
                        .background(MaterialTheme.colorScheme.onSurfaceVariant)
                )
            }
        }
    }
}