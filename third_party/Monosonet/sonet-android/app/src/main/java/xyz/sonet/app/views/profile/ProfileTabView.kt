package xyz.sonet.app.views.profile

import androidx.compose.runtime.Composable
import xyz.sonet.app.ui.profile.ProfileView

@Composable
fun ProfileTabView(
    sessionViewModel: xyz.sonet.app.viewmodels.SessionViewModel,
    themeViewModel: xyz.sonet.app.viewmodels.ThemeViewModel
) {
    val currentUser = sessionViewModel.currentUser.collectAsState().value
    val userId = currentUser?.id ?: ""
    ProfileView(userId = userId, isOwnProfile = true)
}