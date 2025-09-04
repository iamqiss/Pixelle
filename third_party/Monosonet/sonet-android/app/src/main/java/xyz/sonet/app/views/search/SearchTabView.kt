package xyz.sonet.app.views.search

import androidx.compose.runtime.Composable
import xyz.sonet.app.ui.search.SearchView

@Composable
fun SearchTabView(
    sessionViewModel: xyz.sonet.app.viewmodels.SessionViewModel,
    themeViewModel: xyz.sonet.app.viewmodels.ThemeViewModel
) {
    SearchView()
}