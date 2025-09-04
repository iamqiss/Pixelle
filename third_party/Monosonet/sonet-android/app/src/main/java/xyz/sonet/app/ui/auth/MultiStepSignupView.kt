package xyz.sonet.app.ui.auth

import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.grid.GridCells
import androidx.compose.foundation.lazy.grid.LazyVerticalGrid
import androidx.compose.foundation.lazy.grid.items
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.KeyboardOptions
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
import androidx.compose.ui.text.input.ImeAction
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.compose.ui.text.input.VisualTransformation
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.lifecycle.viewmodel.compose.viewModel
import coil.compose.AsyncImage
import coil.request.ImageRequest
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.*

@Composable
fun MultiStepSignupView(
    modifier: Modifier = Modifier,
    viewModel: MultiStepSignupViewModel = viewModel(),
    onSignupComplete: () -> Unit = {}
) {
    var currentStep by remember { mutableStateOf(0) }
    val totalSteps = 5
    
    LaunchedEffect(viewModel.isSignupComplete) {
        if (viewModel.isSignupComplete) {
            onSignupComplete()
        }
    }
    
    Column(
        modifier = modifier.fillMaxSize()
    ) {
        // Progress indicator
        SignupProgressView(
            currentStep = currentStep,
            totalSteps = totalSteps
        )
        
        // Step content
        when (currentStep) {
            0 -> BasicInfoStepView(
                displayName = viewModel.displayName,
                onDisplayNameChange = { viewModel.displayName = it },
                username = viewModel.username,
                onUsernameChange = { viewModel.username = it },
                password = viewModel.password,
                onPasswordChange = { viewModel.password = it },
                passwordConfirmation = viewModel.passwordConfirmation,
                onPasswordConfirmationChange = { viewModel.passwordConfirmation = it },
                onNext = { currentStep++ }
            )
            
            1 -> ComplianceStepView(
                birthday = viewModel.birthday,
                onBirthdayChange = { viewModel.birthday = it },
                email = viewModel.email,
                onEmailChange = { viewModel.email = it },
                phoneNumber = viewModel.phoneNumber,
                onPhoneNumberChange = { viewModel.phoneNumber = it },
                onNext = { currentStep++ },
                onBack = { currentStep-- }
            )
            
            2 -> ProfileCompletionStepView(
                bio = viewModel.bio,
                onBioChange = { viewModel.bio = it },
                avatarImage = viewModel.avatarImage,
                onAvatarImageChange = { viewModel.avatarImage = it },
                onNext = { currentStep++ },
                onBack = { currentStep-- },
                onSkip = { currentStep++ }
            )
            
            3 -> InterestsStepView(
                selectedInterests = viewModel.selectedInterests,
                onInterestsChange = { viewModel.selectedInterests = it },
                onNext = { currentStep++ },
                onBack = { currentStep-- }
            )
            
            4 -> PermissionsStepView(
                contactsAccess = viewModel.contactsAccess,
                onContactsAccessChange = { viewModel.contactsAccess = it },
                locationAccess = viewModel.locationAccess,
                onLocationAccessChange = { viewModel.locationAccess = it },
                onComplete = { viewModel.completeSignup() },
                onBack = { currentStep-- }
            )
        }
    }
}

// MARK: - Progress Indicator
@Composable
fun SignupProgressView(
    currentStep: Int,
    totalSteps: Int
) {
    Column(
        modifier = Modifier
            .fillMaxWidth()
            .padding(20.dp),
        spacing = 16.dp
    ) {
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = Alignment.CenterVertically
        ) {
            Text(
                text = "Step ${currentStep + 1} of $totalSteps",
                fontSize = 14.sp,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
            
            Text(
                text = "${Int((currentStep + 1.0) / totalSteps * 100)}%",
                fontSize = 14.sp,
                fontWeight = FontWeight.SemiBold,
                color = MaterialTheme.colorScheme.primary
            )
        }
        
        // Progress bar
        LinearProgressIndicator(
            progress = (currentStep + 1.0) / totalSteps,
            modifier = Modifier
                .fillMaxWidth()
                .height(4.dp)
                .clip(RoundedCornerShape(2.dp)),
            color = MaterialTheme.colorScheme.primary,
            trackColor = MaterialTheme.colorScheme.surfaceVariant
        )
    }
}

// MARK: - Step 1: Basic Information
@Composable
fun BasicInfoStepView(
    displayName: String,
    onDisplayNameChange: (String) -> Unit,
    username: String,
    onUsernameChange: (String) -> Unit,
    password: String,
    onPasswordChange: (String) -> Unit,
    passwordConfirmation: String,
    onPasswordConfirmationChange: (String) -> Unit,
    onNext: () -> Unit
) {
    var showPassword by remember { mutableStateOf(false) }
    var showPasswordConfirmation by remember { mutableStateOf(false) }
    
    LazyColumn(
        modifier = Modifier.fillMaxSize(),
        contentPadding = PaddingValues(20.dp),
        verticalArrangement = Arrangement.spacedBy(24.dp)
    ) {
        item {
            // Header
            Column(
                horizontalAlignment = Alignment.CenterHorizontally,
                spacing = 12.dp
            ) {
                Icon(
                    imageVector = Icons.Default.PersonAdd,
                    contentDescription = "Signup",
                    modifier = Modifier.size(48.dp),
                    tint = MaterialTheme.colorScheme.primary
                )
                
                Text(
                    text = "Let's get you started!",
                    fontSize = 24.sp,
                    fontWeight = FontWeight.Bold
                )
                
                Text(
                    text = "Create your Sonet account and join millions of people sharing amazing moments.",
                    fontSize = 16.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    textAlign = TextAlign.Center
                )
            }
        }
        
        item {
            // Display Name
            Column(
                verticalArrangement = Arrangement.spacedBy(8.dp)
            ) {
                Text(
                    text = "Display Name",
                    fontSize = 16.sp,
                    fontWeight = FontWeight.SemiBold
                )
                
                OutlinedTextField(
                    value = displayName,
                    onValueChange = onDisplayNameChange,
                    placeholder = { Text("Enter your display name") },
                    modifier = Modifier.fillMaxWidth(),
                    singleLine = true
                )
            }
        }
        
        item {
            // Username
            Column(
                verticalArrangement = Arrangement.spacedBy(8.dp)
            ) {
                Text(
                    text = "Username",
                    fontSize = 16.sp,
                    fontWeight = FontWeight.SemiBold
                )
                
                OutlinedTextField(
                    value = username,
                    onValueChange = onUsernameChange,
                    placeholder = { Text("username") },
                    modifier = Modifier.fillMaxWidth(),
                    singleLine = true,
                    leadingIcon = {
                        Text(
                            text = "@",
                            fontSize = 18.sp,
                            color = MaterialTheme.colorScheme.onSurfaceVariant
                        )
                    }
                )
                
                Text(
                    text = "This will be your unique identifier on Sonet",
                    fontSize = 12.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
        }
        
        item {
            // Password
            Column(
                verticalArrangement = Arrangement.spacedBy(8.dp)
            ) {
                Text(
                    text = "Password",
                    fontSize = 16.sp,
                    fontWeight = FontWeight.SemiBold
                )
                
                OutlinedTextField(
                    value = password,
                    onValueChange = onPasswordChange,
                    placeholder = { Text("Enter your password") },
                    modifier = Modifier.fillMaxWidth(),
                    singleLine = true,
                    visualTransformation = if (showPassword) VisualTransformation.None else PasswordVisualTransformation(),
                    trailingIcon = {
                        IconButton(onClick = { showPassword = !showPassword }) {
                            Icon(
                                imageVector = if (showPassword) Icons.Default.VisibilityOff else Icons.Default.Visibility,
                                contentDescription = "Toggle password visibility"
                            )
                        }
                    }
                )
                
                Text(
                    text = "Use at least 8 characters with a mix of letters, numbers, and symbols",
                    fontSize = 12.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
        }
        
        item {
            // Password Confirmation
            Column(
                verticalArrangement = Arrangement.spacedBy(8.dp)
            ) {
                Text(
                    text = "Confirm Password",
                    fontSize = 16.sp,
                    fontWeight = FontWeight.SemiBold
                )
                
                OutlinedTextField(
                    value = passwordConfirmation,
                    onValueChange = onPasswordConfirmationChange,
                    placeholder = { Text("Confirm your password") },
                    modifier = Modifier.fillMaxWidth(),
                    singleLine = true,
                    visualTransformation = if (showPasswordConfirmation) VisualTransformation.None else PasswordVisualTransformation(),
                    trailingIcon = {
                        IconButton(onClick = { showPasswordConfirmation = !showPasswordConfirmation }) {
                            Icon(
                                imageVector = if (showPasswordConfirmation) Icons.Default.VisibilityOff else Icons.Default.Visibility,
                                contentDescription = "Toggle password confirmation visibility"
                            )
                        }
                    }
                )
            }
        }
        
        item {
            // Continue button
            Button(
                onClick = onNext,
                enabled = canProceed(
                    displayName, username, password, passwordConfirmation
                ),
                modifier = Modifier
                    .fillMaxWidth()
                    .height(56.dp),
                shape = RoundedCornerShape(12.dp)
            ) {
                Text(
                    text = "Continue",
                    fontSize = 16.sp,
                    fontWeight = FontWeight.SemiBold
                )
            }
        }
    }
}

// MARK: - Step 2: Compliance & Contact
@Composable
fun ComplianceStepView(
    birthday: LocalDate,
    onBirthdayChange: (LocalDate) -> Unit,
    email: String,
    onEmailChange: (String) -> Unit,
    phoneNumber: String,
    onPhoneNumberChange: (String) -> Unit,
    onNext: () -> Unit,
    onBack: () -> Unit
) {
    var showDatePicker by remember { mutableStateOf(false) }
    var isAgeValid by remember { mutableStateOf(true) }
    
    LaunchedEffect(birthday) {
        val age = LocalDate.now().year - birthday.year
        isAgeValid = age >= 13
    }
    
    LazyColumn(
        modifier = Modifier.fillMaxSize(),
        contentPadding = PaddingValues(20.dp),
        verticalArrangement = Arrangement.spacedBy(24.dp)
    ) {
        item {
            // Header
            Column(
                horizontalAlignment = Alignment.CenterHorizontally,
                spacing = 12.dp
            ) {
                Icon(
                    imageVector = Icons.Default.Shield,
                    contentDescription = "Compliance",
                    modifier = Modifier.size(48.dp),
                    tint = MaterialTheme.colorScheme.tertiary
                )
                
                Text(
                    text = "Almost there!",
                    fontSize = 24.sp,
                    fontWeight = FontWeight.Bold
                )
                
                Text(
                    text = "We need a few more details to keep Sonet safe and secure for everyone.",
                    fontSize = 16.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    textAlign = TextAlign.Center
                )
            }
        }
        
        item {
            // Birthday
            Column(
                verticalArrangement = Arrangement.spacedBy(8.dp)
            ) {
                Text(
                    text = "Birthday",
                    fontSize = 16.sp,
                    fontWeight = FontWeight.SemiBold
                )
                
                OutlinedTextField(
                    value = birthday.format(DateTimeFormatter.ofPattern("MMM dd, yyyy")),
                    onValueChange = { },
                    readOnly = true,
                    modifier = Modifier
                        .fillMaxWidth()
                        .clickable { showDatePicker = true },
                    trailingIcon = {
                        Icon(
                            imageVector = Icons.Default.DateRange,
                            contentDescription = "Select date"
                        )
                    }
                )
                
                if (!isAgeValid) {
                    Text(
                        text = "You must be at least 13 years old to use Sonet",
                        fontSize = 12.sp,
                        color = MaterialTheme.colorScheme.error
                    )
                }
            }
        }
        
        item {
            // Email
            Column(
                verticalArrangement = Arrangement.spacedBy(8.dp)
            ) {
                Text(
                    text = "Email Address",
                    fontSize = 16.sp,
                    fontWeight = FontWeight.SemiBold
                )
                
                OutlinedTextField(
                    value = email,
                    onValueChange = onEmailChange,
                    placeholder = { Text("Enter your email address") },
                    modifier = Modifier.fillMaxWidth(),
                    singleLine = true,
                    keyboardOptions = KeyboardOptions(
                        keyboardType = KeyboardType.Email,
                        imeAction = ImeAction.Next
                    )
                )
                
                Text(
                    text = "We'll use this to send you important updates and keep your account secure",
                    fontSize = 12.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
        }
        
        item {
            // Phone Number
            Column(
                verticalArrangement = Arrangement.spacedBy(8.dp)
            ) {
                Text(
                    text = "Phone Number (Optional)",
                    fontSize = 16.sp,
                    fontWeight = FontWeight.SemiBold
                )
                
                OutlinedTextField(
                    value = phoneNumber,
                    onValueChange = onPhoneNumberChange,
                    placeholder = { Text("Enter your phone number") },
                    modifier = Modifier.fillMaxWidth(),
                    singleLine = true,
                    keyboardOptions = KeyboardOptions(
                        keyboardType = KeyboardType.Phone,
                        imeAction = ImeAction.Done
                    )
                )
                
                Text(
                    text = "This helps us recommend friends and keep your account secure. You can skip this step.",
                    fontSize = 12.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
        }
        
        item {
            // Navigation buttons
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.spacedBy(16.dp)
            ) {
                OutlinedButton(
                    onClick = onBack,
                    modifier = Modifier.weight(1f),
                    shape = RoundedCornerShape(12.dp)
                ) {
                    Text("Back")
                }
                
                Button(
                    onClick = onNext,
                    enabled = isAgeValid && email.isNotBlank(),
                    modifier = Modifier.weight(1f),
                    shape = RoundedCornerShape(12.dp)
                ) {
                    Text("Continue")
                }
            }
        }
    }
    
    if (showDatePicker) {
        DatePickerDialog(
            onDismissRequest = { showDatePicker = false },
            confirmButton = {
                TextButton(onClick = { showDatePicker = false }) {
                    Text("OK")
                }
            },
            dismissButton = {
                TextButton(onClick = { showDatePicker = false }) {
                    Text("Cancel")
                }
            }
        ) {
            DatePicker(
                state = rememberDatePickerState(
                    initialSelectedDateMillis = birthday.toEpochDay() * 24 * 60 * 60 * 1000
                ),
                showModeToggle = false
            )
        }
    }
}

// MARK: - Step 3: Profile Completion (Optional)
@Composable
fun ProfileCompletionStepView(
    bio: String,
    onBioChange: (String) -> Unit,
    avatarImage: String?,
    onAvatarImageChange: (String?) -> Unit,
    onNext: () -> Unit,
    onBack: () -> Unit,
    onSkip: () -> Unit
) {
    LazyColumn(
        modifier = Modifier.fillMaxSize(),
        contentPadding = PaddingValues(20.dp),
        verticalArrangement = Arrangement.spacedBy(24.dp)
    ) {
        item {
            // Header
            Column(
                horizontalAlignment = Alignment.CenterHorizontally,
                spacing = 12.dp
            ) {
                Icon(
                    imageVector = Icons.Default.Person,
                    contentDescription = "Profile",
                    modifier = Modifier.size(48.dp),
                    tint = MaterialTheme.colorScheme.secondary
                )
                
                Text(
                    text = "Tell us about yourself!",
                    fontSize = 24.sp,
                    fontWeight = FontWeight.Bold
                )
                
                Text(
                    text = "This step is completely optional. You can always add these details later.",
                    fontSize = 16.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    textAlign = TextAlign.Center
                )
            }
        }
        
        item {
            // Profile Picture
            Column(
                verticalArrangement = Arrangement.spacedBy(8.dp)
            ) {
                Text(
                    text = "Profile Picture",
                    fontSize = 16.sp,
                    fontWeight = FontWeight.SemiBold
                )
                
                Box(
                    modifier = Modifier
                        .size(100.dp)
                        .clip(CircleShape)
                        .background(MaterialTheme.colorScheme.surfaceVariant)
                        .clickable { /* TODO: Implement image picker */ },
                    contentAlignment = Alignment.Center
                ) {
                    if (avatarImage != null) {
                        AsyncImage(
                            model = ImageRequest.Builder(LocalContext.current)
                                .data(avatarImage)
                                .crossfade(true)
                                .build(),
                            contentDescription = "Profile picture",
                            modifier = Modifier.fillMaxSize(),
                            contentScale = ContentScale.Crop
                        )
                    } else {
                        Column(
                            horizontalAlignment = Alignment.CenterHorizontally,
                            verticalArrangement = Arrangement.spacedBy(4.dp)
                        ) {
                            Icon(
                                imageVector = Icons.Default.CameraAlt,
                                contentDescription = "Add photo",
                                modifier = Modifier.size(24.dp),
                                tint = MaterialTheme.colorScheme.onSurfaceVariant
                            )
                            
                            Text(
                                text = "Add Photo",
                                fontSize = 12.sp,
                                color = MaterialTheme.colorScheme.onSurfaceVariant
                            )
                        }
                    }
                }
                
                Text(
                    text = "Add a profile picture to help friends recognize you",
                    fontSize = 12.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
        }
        
        item {
            // Bio
            Column(
                verticalArrangement = Arrangement.spacedBy(8.dp)
            ) {
                Text(
                    text = "Bio",
                    fontSize = 16.sp,
                    fontWeight = FontWeight.SemiBold
                )
                
                OutlinedTextField(
                    value = bio,
                    onValueChange = onBioChange,
                    placeholder = { Text("Tell us about yourself...") },
                    modifier = Modifier.fillMaxWidth(),
                    minLines = 3,
                    maxLines = 6
                )
                
                Text(
                    text = "Share a little about yourself (optional)",
                    fontSize = 12.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
        }
        
        item {
            // Navigation buttons
            Column(
                verticalArrangement = Arrangement.spacedBy(16.dp)
            ) {
                Button(
                    onClick = onNext,
                    modifier = Modifier.fillMaxWidth(),
                    shape = RoundedCornerShape(12.dp)
                ) {
                    Text("Continue")
                }
                
                TextButton(
                    onClick = onSkip,
                    modifier = Modifier.fillMaxWidth()
                ) {
                    Text("Skip for now")
                }
                
                OutlinedButton(
                    onClick = onBack,
                    modifier = Modifier.fillMaxWidth(),
                    shape = RoundedCornerShape(12.dp)
                ) {
                    Text("Back")
                }
            }
        }
    }
}

// MARK: - Step 4: Interests
@Composable
fun InterestsStepView(
    selectedInterests: Set<String>,
    onInterestsChange: (Set<String>) -> Unit,
    onNext: () -> Unit,
    onBack: () -> Unit
) {
    val availableInterests = remember {
        listOf(
            "Technology", "Music", "Sports", "Travel", "Food", "Fashion",
            "Art", "Gaming", "Fitness", "Photography", "Books", "Movies",
            "Science", "Business", "Education", "Health", "Nature", "Cars",
            "Pets", "DIY", "Cooking", "Dance", "Comedy", "News"
        )
    }
    
    LazyColumn(
        modifier = Modifier.fillMaxSize(),
        contentPadding = PaddingValues(20.dp),
        verticalArrangement = Arrangement.spacedBy(24.dp)
    ) {
        item {
            // Header
            Column(
                horizontalAlignment = Alignment.CenterHorizontally,
                spacing = 12.dp
            ) {
                Icon(
                    imageVector = Icons.Default.Favorite,
                    contentDescription = "Interests",
                    modifier = Modifier.size(48.dp),
                    tint = MaterialTheme.colorScheme.error
                )
                
                Text(
                    text = "What interests you?",
                    fontSize = 24.sp,
                    fontWeight = FontWeight.Bold
                )
                
                Text(
                    text = "Help us personalize your experience and connect you with like-minded people.",
                    fontSize = 16.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    textAlign = TextAlign.Center
                )
            }
        }
        
        item {
            // Interests grid
            LazyVerticalGrid(
                columns = GridCells.Fixed(2),
                horizontalArrangement = Arrangement.spacedBy(12.dp),
                verticalArrangement = Arrangement.spacedBy(12.dp)
            ) {
                items(availableInterests) { interest ->
                    InterestChip(
                        title = interest,
                        isSelected = selectedInterests.contains(interest),
                        onTap = {
                            val newInterests = selectedInterests.toMutableSet()
                            if (newInterests.contains(interest)) {
                                newInterests.remove(interest)
                            } else {
                                newInterests.add(interest)
                            }
                            onInterestsChange(newInterests)
                        }
                    )
                }
            }
        }
        
        item {
            // Navigation buttons
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.spacedBy(16.dp)
            ) {
                OutlinedButton(
                    onClick = onBack,
                    modifier = Modifier.weight(1f),
                    shape = RoundedCornerShape(12.dp)
                ) {
                    Text("Back")
                }
                
                Button(
                    onClick = onNext,
                    enabled = selectedInterests.size >= 3,
                    modifier = Modifier.weight(1f),
                    shape = RoundedCornerShape(12.dp)
                ) {
                    Text("Continue")
                }
            }
        }
    }
}

// MARK: - Step 5: Permissions
@Composable
fun PermissionsStepView(
    contactsAccess: Boolean,
    onContactsAccessChange: (Boolean) -> Unit,
    locationAccess: Boolean,
    onLocationAccessChange: (Boolean) -> Unit,
    onComplete: () -> Unit,
    onBack: () -> Unit
) {
    LazyColumn(
        modifier = Modifier.fillMaxSize(),
        contentPadding = PaddingValues(20.dp),
        verticalArrangement = Arrangement.spacedBy(24.dp)
    ) {
        item {
            // Header
            Column(
                horizontalAlignment = Alignment.CenterHorizontally,
                spacing = 12.dp
            ) {
                Icon(
                    imageVector = Icons.Default.Handshake,
                    contentDescription = "Permissions",
                    modifier = Modifier.size(48.dp),
                    tint = MaterialTheme.colorScheme.tertiary
                )
                
                Text(
                    text = "Last step!",
                    fontSize = 24.sp,
                    fontWeight = FontWeight.Bold
                )
                
                Text(
                    text = "Help us make Sonet better for you by granting a few permissions.",
                    fontSize = 16.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    textAlign = TextAlign.Center
                )
            }
        }
        
        item {
            // Permissions
            Column(
                verticalArrangement = Arrangement.spacedBy(20.dp)
            ) {
                // Contacts Access
                PermissionCard(
                    icon = Icons.Default.People,
                    title = "Find Friends",
                    description = "Connect with people you know by syncing your contacts. We'll help you discover friends who are already on Sonet!",
                    isEnabled = contactsAccess,
                    onToggle = onContactsAccessChange
                )
                
                // Location Access
                PermissionCard(
                    icon = Icons.Default.LocationOn,
                    title = "Discover Nearby",
                    description = "Find interesting people and content near you. We'll recommend local creators and trending topics in your area.",
                    isEnabled = locationAccess,
                    onToggle = onLocationAccessChange
                )
            }
        }
        
        item {
            // Trust message
            Column(
                horizontalAlignment = Alignment.CenterHorizontally,
                spacing = 12.dp
            ) {
                Icon(
                    imageVector = Icons.Default.Security,
                    contentDescription = "Privacy",
                    modifier = Modifier.size(24.dp),
                    tint = MaterialTheme.colorScheme.tertiary
                )
                
                Text(
                    text = "Your privacy is important to us",
                    fontSize = 16.sp,
                    fontWeight = FontWeight.SemiBold
                )
                
                Text(
                    text = "You can always change these permissions in Settings. We never share your personal data with third parties.",
                    fontSize = 12.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    textAlign = TextAlign.Center
                )
            }
        }
        
        item {
            // Navigation buttons
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.spacedBy(16.dp)
            ) {
                OutlinedButton(
                    onClick = onBack,
                    modifier = Modifier.weight(1f),
                    shape = RoundedCornerShape(12.dp)
                ) {
                    Text("Back")
                }
                
                Button(
                    onClick = onComplete,
                    modifier = Modifier.weight(1f),
                    shape = RoundedCornerShape(12.dp)
                ) {
                    Text("Complete Signup")
                }
            }
        }
    }
}

// MARK: - Supporting Views
@Composable
fun InterestChip(
    title: String,
    isSelected: Boolean,
    onTap: () -> Unit
) {
    Box(
        modifier = Modifier
            .fillMaxWidth()
            .clip(RoundedCornerShape(20.dp))
            .background(
                if (isSelected) MaterialTheme.colorScheme.primary
                else MaterialTheme.colorScheme.surfaceVariant
            )
            .border(
                width = if (isSelected) 2.dp else 0.dp,
                color = if (isSelected) MaterialTheme.colorScheme.primary else Color.Transparent,
                shape = RoundedCornerShape(20.dp)
            )
            .clickable { onTap() }
            .padding(horizontal = 16.dp, vertical = 12.dp),
        contentAlignment = Alignment.Center
    ) {
        Text(
            text = title,
            fontSize = 14.sp,
            fontWeight = FontWeight.Medium,
            color = if (isSelected) MaterialTheme.colorScheme.onPrimary
                   else MaterialTheme.colorScheme.onSurfaceVariant
        )
    }
}

@Composable
fun PermissionCard(
    icon: ImageVector,
    title: String,
    description: String,
    isEnabled: Boolean,
    onToggle: (Boolean) -> Unit
) {
    Card(
        modifier = Modifier.fillMaxWidth(),
        colors = CardDefaults.cardColors(
            containerColor = MaterialTheme.colorScheme.surfaceVariant
        )
    ) {
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(16.dp),
            verticalAlignment = Alignment.CenterVertically,
            horizontalArrangement = Arrangement.spacedBy(12.dp)
        ) {
            Icon(
                imageVector = icon,
                contentDescription = title,
                modifier = Modifier.size(24.dp),
                tint = MaterialTheme.colorScheme.primary
            )
            
            Column(
                modifier = Modifier.weight(1f),
                verticalArrangement = Arrangement.spacedBy(4.dp)
            ) {
                Text(
                    text = title,
                    fontSize = 16.sp,
                    fontWeight = FontWeight.SemiBold
                )
                
                Text(
                    text = description,
                    fontSize = 14.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
            
            Switch(
                checked = isEnabled,
                onCheckedChange = onToggle
            )
        }
    }
}

// MARK: - Helper Functions
private fun canProceed(
    displayName: String,
    username: String,
    password: String,
    passwordConfirmation: String
): Boolean {
    return displayName.trim().isNotBlank() &&
           username.trim().isNotBlank() &&
           password.length >= 8 &&
           password == passwordConfirmation
}