package xyz.sonet.app.ui.settings

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.lifecycle.viewmodel.compose.viewModel
import xyz.sonet.app.viewmodels.*

@Composable
fun LocationPrivacyView(vm: PrivacyViewModel = viewModel()) {
    val enabled by vm.locationServicesEnabled.collectAsState()
    val precise by vm.sharePreciseLocation.collectAsState()
    val allowPosts by vm.allowLocationOnPosts.collectAsState()
    val geotag by vm.defaultGeotagPrivacy.collectAsState()
    val history by vm.locationHistory.collectAsState()

    LaunchedEffect(Unit) { vm.loadLocationPrivacy() }

    LazyColumn(modifier = Modifier.fillMaxSize()) {
        item {
            SettingsSection(title = "Location Sharing", subtitle = "Control how location is used") {}
        }
        item {
            SettingsToggleItem(icon = Icons.Default.LocationOn, title = "Enable Location Services", checked = enabled) {
                vm.setLocationServicesEnabled(!enabled)
            }
        }
        item {
            SettingsToggleItem(icon = Icons.Default.Place, title = "Share Precise Location", checked = precise) {
                vm.setSharePreciseLocation(!precise)
            }
        }
        item {
            SettingsToggleItem(icon = Icons.Default.Map, title = "Allow Location on Posts by Default", checked = allowPosts) {
                vm.setAllowLocationOnPosts(!allowPosts)
            }
        }
        item {
            Text("Default Geotag Privacy", modifier = Modifier.padding(horizontal = 16.dp, vertical = 8.dp), style = MaterialTheme.typography.bodyMedium, fontWeight = FontWeight.Medium)
            GeotagPrivacyPicker(selected = geotag) { vm.setDefaultGeotagPrivacy(it) }
        }
        item {
            Text("Location History", modifier = Modifier.padding(16.dp))
        }
        items(history) { item ->
            ListItem(headlineContent = { Text(item.name) }, supportingContent = { Text(item.timestamp.toString()) })
            Divider()
        }
        item {
            Row(modifier = Modifier.fillMaxWidth().padding(16.dp), horizontalArrangement = Arrangement.End) {
                OutlinedButton(onClick = { vm.clearLocationHistory() }) { Text("Clear Location History") }
            }
        }
    }
}

@Composable
private fun GeotagPrivacyPicker(selected: PostPrivacy, onSelect: (PostPrivacy) -> Unit) {
    Column(modifier = Modifier.padding(horizontal = 16.dp)) {
        PostPrivacy.values().forEach { privacy ->
            Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.fillMaxWidth().padding(vertical = 8.dp)) {
                RadioButton(selected = selected == privacy, onClick = { onSelect(privacy) })
                Spacer(Modifier.width(8.dp))
                Text(privacy.displayName)
            }
        }
    }
}

@Composable
fun ThirdPartyAppsView(vm: PrivacyViewModel = viewModel()) {
    val apps by vm.connectedApps.collectAsState()

    LaunchedEffect(Unit) { vm.loadConnectedApps() }

    LazyColumn(modifier = Modifier.fillMaxSize()) {
        items(apps) { app ->
            ListItem(
                headlineContent = { Text(app.name) },
                supportingContent = { Text("Scopes: ${app.scopes.joinToString(", ")}") },
                trailingContent = {
                    OutlinedButton(onClick = { vm.revokeApp(app.id) }) { Text("Revoke") }
                }
            )
            Divider()
        }
        if (apps.isEmpty()) {
            item { Text("No connected apps", modifier = Modifier.padding(16.dp), color = MaterialTheme.colorScheme.onSurfaceVariant) }
        }
    }
}

@Composable
fun DataExportView(vm: PrivacyViewModel = viewModel()) {
    val exports by vm.previousExports.collectAsState()
    var includeMedia by remember { mutableStateOf(true) }
    var includeMessages by remember { mutableStateOf(true) }
    var includeConnections by remember { mutableStateOf(true) }
    var format by remember { mutableStateOf(DataExportFormat.JSON) }
    var range by remember { mutableStateOf(DataExportRange.ALL_TIME) }

    LaunchedEffect(Unit) { vm.loadPreviousExports() }

    LazyColumn(modifier = Modifier.fillMaxSize()) {
        item { SettingsSection(title = "What to include", subtitle = "Choose data to export") {} }
        item { SettingsToggleItem(icon = Icons.Default.Description, title = "Posts & Activity", checked = true, onToggle = {}) }
        item { SettingsToggleItem(icon = Icons.Default.Image, title = "Media (photos, videos)", checked = includeMedia) { includeMedia = !includeMedia } }
        item { SettingsToggleItem(icon = Icons.Default.Message, title = "Messages", checked = includeMessages) { includeMessages = !includeMessages } }
        item { SettingsToggleItem(icon = Icons.Default.Group, title = "Connections", checked = includeConnections) { includeConnections = !includeConnections } }
        item { Divider() }
        item { SettingsSection(title = "Export options", subtitle = "Format and time range") {} }
        item { PickerRow("Format", format.name) { /* simple cycle */ format = when(format){ DataExportFormat.JSON->DataExportFormat.CSV; DataExportFormat.CSV->DataExportFormat.HTML; DataExportFormat.HTML->DataExportFormat.JSON } } }
        item { PickerRow("Time range", range.name) { range = when(range){ DataExportRange.ALL_TIME->DataExportRange.LAST_YEAR; DataExportRange.LAST_YEAR->DataExportRange.LAST_MONTH; DataExportRange.LAST_MONTH->DataExportRange.ALL_TIME } } }
        item {
            Row(modifier = Modifier.fillMaxWidth().padding(16.dp), horizontalArrangement = Arrangement.End) {
                Button(onClick = { vm.requestExport(includeMedia, includeMessages, includeConnections, format, range) }) { Text("Request Export") }
            }
        }
        item { Divider() }
        item { Text("Previous exports", modifier = Modifier.padding(16.dp)) }
        items(exports) { exp ->
            ListItem(
                headlineContent = { Text("Requested: ${exp.requestedAt}") },
                supportingContent = { Text("Status: ${exp.status}") },
                trailingContent = {
                    if (exp.status == DataExportStatus.READY) Button(onClick = { vm.downloadExport(exp.id) }) { Text("Download") }
                }
            )
            Divider()
        }
        if (exports.isEmpty()) { item { Text("No previous exports", modifier = Modifier.padding(16.dp), color = MaterialTheme.colorScheme.onSurfaceVariant) } }
    }
}

@Composable
private fun PickerRow(title: String, value: String, onClick: () -> Unit) {
    ListItem(headlineContent = { Text(title) }, trailingContent = { Text(value) }, modifier = Modifier
        .fillMaxWidth()
        .padding(horizontal = 8.dp)
        .let { it })
}