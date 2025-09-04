package xyz.sonet.app.ui.auth

import androidx.compose.animation.core.*
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.draw.scale
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.navigation.NavController

@Composable
fun WelcomeView(
    navController: NavController,
    modifier: Modifier = Modifier
) {
    var animateGradient by remember { mutableStateOf(false) }
    
    val infiniteTransition = rememberInfiniteTransition(label = "gradient")
    val gradientAnimation by infiniteTransition.animateFloat(
        initialValue = 0f,
        targetValue = 1f,
        animationSpec = infiniteRepeatable(
            animation = tween(3000, easing = LinearEasing),
            repeatMode = RepeatMode.Reverse
        ),
        label = "gradient"
    )
    
    val scaleAnimation by infiniteTransition.animateFloat(
        initialValue = 1f,
        targetValue = 1.1f,
        animationSpec = infiniteRepeatable(
            animation = tween(2000, easing = LinearEasing),
            repeatMode = RepeatMode.Reverse
        ),
        label = "scale"
    )
    
    LaunchedEffect(Unit) {
        animateGradient = true
    }
    
    Box(
        modifier = modifier.fillMaxSize()
    ) {
        // Animated gradient background
        Box(
            modifier = Modifier
                .fillMaxSize()
                .background(
                    Brush.linearGradient(
                        colors = listOf(
                            MaterialTheme.colorScheme.background,
                            MaterialTheme.colorScheme.surface,
                            MaterialTheme.colorScheme.background
                        ),
                        start = if (gradientAnimation < 0.5f) 
                            androidx.compose.ui.geometry.Offset(0f, 0f)
                        else 
                            androidx.compose.ui.geometry.Offset(1f, 1f),
                        end = if (gradientAnimation < 0.5f) 
                            androidx.compose.ui.geometry.Offset(1f, 1f)
                        else 
                            androidx.compose.ui.geometry.Offset(0f, 0f)
                    )
                )
        )
        
        // Content
        Column(
            modifier = Modifier
                .fillMaxSize()
                .verticalScroll(rememberScrollState())
                .padding(20.dp),
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.SpaceBetween
        ) {
            Spacer(modifier = Modifier.height(60.dp))
            
            // Sonet Logo & Branding
            Column(
                horizontalAlignment = Alignment.CenterHorizontally,
                spacing = 24.dp
            ) {
                // Logo
                Box(
                    modifier = Modifier
                        .size(120.dp)
                        .clip(CircleShape)
                        .background(MaterialTheme.colorScheme.onBackground.copy(alpha = 0.2f)),
                    contentAlignment = Alignment.Center
                ) {
                    Icon(
                        imageVector = Icons.Default.NetworkCheck,
                        contentDescription = "Sonet Logo",
                        modifier = Modifier
                            .size(60.dp)
                            .scale(scaleAnimation),
                        tint = MaterialTheme.colorScheme.onBackground
                    )
                }
                
                // App Name
                Text(
                    text = "Sonet",
                    fontSize = 48.sp,
                    fontWeight = FontWeight.Bold,
                    color = MaterialTheme.colorScheme.onBackground
                )
                
                // Tagline
                Text(
                    text = "Connect. Share. Discover.",
                    fontSize = 20.sp,
                    fontWeight = FontWeight.Medium,
                    color = MaterialTheme.colorScheme.onBackground.copy(alpha = 0.9f),
                    textAlign = TextAlign.Center
                )
            }
            
            // Features Preview
            Column(
                spacing = 20.dp
            ) {
                FeatureRow(
                    icon = Icons.Default.VideoLibrary,
                    title = "TikTok-style Videos",
                    description = "Create and discover amazing short-form content"
                )
                
                FeatureRow(
                    icon = Icons.Default.Message,
                    title = "Smart Messaging",
                    description = "Connect with friends through intelligent conversations"
                )
                
                FeatureRow(
                    icon = Icons.Default.People,
                    title = "Social Discovery",
                    description = "Find people who share your interests and passions"
                )
            }
            
            Spacer(modifier = Modifier.height(40.dp))
            
            // Action Buttons
            Column(
                spacing = 16.dp
            ) {
                // Get Started Button
                Button(
                    onClick = { navController.navigate("multiStepSignup") },
                    modifier = Modifier
                        .fillMaxWidth()
                        .height(64.dp),
                    shape = RoundedCornerShape(16.dp),
                    colors = ButtonDefaults.buttonColors(
                        containerColor = MaterialTheme.colorScheme.onBackground
                    )
                ) {
                    Row(
                        horizontalArrangement = Arrangement.spacedBy(8.dp),
                        verticalAlignment = Alignment.CenterVertically
                    ) {
                        Text(
                            text = "Get Started",
                            fontSize = 18.sp,
                            fontWeight = FontWeight.SemiBold,
                            color = MaterialTheme.colorScheme.background
                        )
                        
                        Icon(
                            imageVector = Icons.Default.ArrowForward,
                            contentDescription = "Get Started",
                            tint = MaterialTheme.colorScheme.background
                        )
                    }
                }
                
                // Sign In Button
                TextButton(
                    onClick = { navController.navigate("login") },
                    modifier = Modifier.fillMaxWidth()
                ) {
                    Text(
                        text = "Already have an account? Sign In",
                        fontSize = 16.sp,
                        fontWeight = FontWeight.Medium,
                        color = MaterialTheme.colorScheme.onBackground
                    )
                }
            }
            
            Spacer(modifier = Modifier.height(40.dp))
        }
    }
}

// MARK: - Feature Row
@Composable
fun FeatureRow(
    icon: ImageVector,
    title: String,
    description: String
) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .padding(horizontal = 20.dp, vertical = 16.dp)
            .background(
                color = MaterialTheme.colorScheme.onBackground.copy(alpha = 0.1f),
                shape = RoundedCornerShape(16.dp)
            )
            .border(
                width = 1.dp,
                color = MaterialTheme.colorScheme.onBackground.copy(alpha = 0.2f),
                shape = RoundedCornerShape(16.dp)
            ),
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.spacedBy(16.dp)
    ) {
        // Icon
        Box(
            modifier = Modifier
                .size(50.dp)
                .clip(CircleShape)
                .background(MaterialTheme.colorScheme.onBackground.copy(alpha = 0.2f)),
            contentAlignment = Alignment.Center
        ) {
            Icon(
                imageVector = icon,
                contentDescription = title,
                modifier = Modifier.size(24.dp),
                tint = MaterialTheme.colorScheme.onBackground
            )
        }
        
        // Text
        Column(
            modifier = Modifier.weight(1f),
            verticalArrangement = Arrangement.spacedBy(4.dp)
        ) {
            Text(
                text = title,
                fontSize = 18.sp,
                fontWeight = FontWeight.SemiBold,
                color = MaterialTheme.colorScheme.onBackground
            )
            
            Text(
                text = description,
                fontSize = 14.sp,
                color = MaterialTheme.colorScheme.onBackground.copy(alpha = 0.8f),
                textAlign = TextAlign.Start
            )
        }
    }
}

// MARK: - Login View (Placeholder)
@Composable
fun LoginView(
    navController: NavController,
    modifier: Modifier = Modifier
) {
    Column(
        modifier = modifier
            .fillMaxSize()
            .padding(20.dp),
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.spacedBy(24.dp)
    ) {
        Spacer(modifier = Modifier.height(40.dp))
        
        Text(
            text = "Sign In",
            fontSize = 32.sp,
            fontWeight = FontWeight.Bold
        )
        
        Text(
            text = "Welcome back! Sign in to your Sonet account.",
            fontSize = 16.sp,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            textAlign = TextAlign.Center
        )
        
        // Placeholder for login form
        Column(
            spacing = 16.dp
        ) {
            OutlinedTextField(
                value = "",
                onValueChange = { },
                placeholder = { Text("Email or Username") },
                modifier = Modifier.fillMaxWidth(),
                singleLine = true
            )
            
            OutlinedTextField(
                value = "",
                onValueChange = { },
                placeholder = { Text("Password") },
                modifier = Modifier.fillMaxWidth(),
                singleLine = true
            )
            
            Button(
                onClick = { /* TODO: Implement login */ },
                modifier = Modifier.fillMaxWidth()
            ) {
                Text("Sign In")
            }
        }
        
        Spacer(modifier = Modifier.weight(1f))
        
        // Back button
        OutlinedButton(
            onClick = { navController.popBackStack() },
            modifier = Modifier.fillMaxWidth()
        ) {
            Text("Back to Welcome")
        }
    }
}