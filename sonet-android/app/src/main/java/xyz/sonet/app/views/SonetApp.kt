package xyz.sonet.app.views

import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import androidx.lifecycle.viewmodel.compose.viewModel
import androidx.navigation.compose.NavHost
import androidx.navigation.compose.composable
import androidx.navigation.compose.rememberNavController
import xyz.sonet.app.viewmodels.AppViewModel
import xyz.sonet.app.viewmodels.SessionViewModel
import xyz.sonet.app.viewmodels.ThemeViewModel
import xyz.sonet.app.views.auth.AuthenticationView
import xyz.sonet.app.views.home.HomeTabView
import xyz.sonet.app.views.messages.MessagesTabView
import xyz.sonet.app.views.notifications.NotificationsTabView
import xyz.sonet.app.views.profile.ProfileTabView
import xyz.sonet.app.ui.video.VideoFeedView

@Composable
fun SonetApp(
    appViewModel: AppViewModel = viewModel(),
    sessionViewModel: SessionViewModel = viewModel(),
    themeViewModel: ThemeViewModel = viewModel()
) {
    val navController = rememberNavController()
    
    // Observe authentication state
    val isAuthenticated by sessionViewModel.isAuthenticated.collectAsState()
    
    LaunchedEffect(Unit) {
        appViewModel.initialize()
        sessionViewModel.initialize()
        themeViewModel.initialize()
    }
    
    if (isAuthenticated) {
        // Main app with bottom navigation
        MainTabView(
            navController = navController,
            sessionViewModel = sessionViewModel,
            themeViewModel = themeViewModel
        )
    } else {
        // Authentication flow
        AuthenticationView(
            sessionViewModel = sessionViewModel,
            onAuthenticationSuccess = {
                // Navigation will be handled automatically by state change
            }
        )
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun MainTabView(
    navController: androidx.navigation.NavController,
    sessionViewModel: SessionViewModel,
    themeViewModel: ThemeViewModel
) {
    var selectedTab by remember { mutableStateOf(0) }
    
    Scaffold(
        bottomBar = {
            NavigationBar {
                NavigationBarItem(
                    icon = { xyz.sonet.app.ui.AppIcons.Home },
                    label = { androidx.compose.material3.Text("Home") },
                    selected = selectedTab == 0,
                    onClick = { selectedTab = 0 }
                )
                NavigationBarItem(
                    icon = { xyz.sonet.app.ui.AppIcons.Video },
                    label = { androidx.compose.material3.Text("Video") },
                    selected = selectedTab == 1,
                    onClick = { selectedTab = 1 }
                )
                NavigationBarItem(
                    icon = { xyz.sonet.app.ui.AppIcons.Messages },
                    label = { androidx.compose.material3.Text("Messages") },
                    selected = selectedTab == 2,
                    onClick = { selectedTab = 2 }
                )
                NavigationBarItem(
                    icon = { xyz.sonet.app.ui.AppIcons.Notifications },
                    label = { androidx.compose.material3.Text("Notifications") },
                    selected = selectedTab == 3,
                    onClick = { selectedTab = 3 }
                )
                NavigationBarItem(
                    icon = { xyz.sonet.app.ui.AppIcons.Profile },
                    label = { androidx.compose.material3.Text("Profile") },
                    selected = selectedTab == 4,
                    onClick = { selectedTab = 4 }
                )
            }
        }
    ) { paddingValues ->
        Box(
            modifier = Modifier
                .fillMaxSize()
                .padding(paddingValues)
        ) {
            when (selectedTab) {
                0 -> HomeTabView(
                    sessionViewModel = sessionViewModel,
                    themeViewModel = themeViewModel
                )
                1 -> VideoFeedView()
                2 -> MessagesTabView(
                    sessionViewModel = sessionViewModel,
                    themeViewModel = themeViewModel
                )
                3 -> NotificationsTabView(
                    sessionViewModel = sessionViewModel,
                    themeViewModel = themeViewModel
                )
                4 -> ProfileTabView(
                    sessionViewModel = sessionViewModel,
                    themeViewModel = themeViewModel
                )
            }
        }
    }
}