package xyz.sonet.app.ui.settings

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.compose.ui.unit.dp
import androidx.lifecycle.viewmodel.compose.viewModel
import xyz.sonet.app.viewmodels.AccountRecoveryViewModel

@Composable
fun AccountRecoveryView(vm: AccountRecoveryViewModel = viewModel()) {
    val email by vm.recoveryEmail.collectAsState()
    val emailVerified by vm.emailVerified.collectAsState()
    val phone by vm.recoveryPhone.collectAsState()
    val phoneVerified by vm.phoneVerified.collectAsState()
    val backupCodes by vm.backupCodes.collectAsState()
    val message by vm.message.collectAsState()
    val error by vm.error.collectAsState()

    var newPassword by remember { mutableStateOf("") }
    var confirmPassword by remember { mutableStateOf("") }

    LaunchedEffect(Unit) { vm.load() }

    LazyColumn(modifier = Modifier.fillMaxSize()) {
        item { SettingsSection(title = "Recovery Email", subtitle = "Verify to enable email recovery") {} }
        item {
            OutlinedTextField(value = email, onValueChange = { vm.setEmail(it) }, label = { Text("Email address") }, modifier = Modifier
                .fillMaxWidth()
                .padding(16.dp))
        }
        item {
            Row(modifier = Modifier.fillMaxWidth().padding(horizontal = 16.dp), horizontalArrangement = Arrangement.End) {
                Button(onClick = { vm.verifyEmail() }, enabled = email.contains("@") && email.contains(".")) { Text("Verify Email") }
            }
        }
        if (emailVerified) item { Text("Email verified", modifier = Modifier.padding(16.dp), color = MaterialTheme.colorScheme.primary) }

        item { Divider() }
        item { SettingsSection(title = "Recovery Phone", subtitle = "Verify to enable phone recovery") {} }
        item {
            OutlinedTextField(value = phone, onValueChange = { vm.setPhone(it) }, label = { Text("Phone number") }, modifier = Modifier
                .fillMaxWidth()
                .padding(16.dp))
        }
        item {
            Row(modifier = Modifier.fillMaxWidth().padding(horizontal = 16.dp), horizontalArrangement = Arrangement.End) {
                Button(onClick = { vm.verifyPhone() }, enabled = phone.length >= 7) { Text("Verify Phone") }
            }
        }
        if (phoneVerified) item { Text("Phone verified", modifier = Modifier.padding(16.dp), color = MaterialTheme.colorScheme.primary) }

        item { Divider() }
        item { SettingsSection(title = "Backup Codes", subtitle = "Use these if you lose access to your device") {} }
        if (backupCodes.isEmpty()) {
            item {
                Row(modifier = Modifier.fillMaxWidth().padding(16.dp), horizontalArrangement = Arrangement.End) {
                    OutlinedButton(onClick = { vm.generateBackupCodes() }) { Text("Generate Backup Codes") }
                }
            }
        } else {
            items(backupCodes) { code ->
                ListItem(headlineContent = { Text(code) })
                Divider()
            }
            item {
                Row(modifier = Modifier.fillMaxWidth().padding(16.dp), horizontalArrangement = Arrangement.End) {
                    OutlinedButton(onClick = { vm.generateBackupCodes() }) { Text("Regenerate Codes") }
                }
            }
        }

        item { Divider() }
        item { SettingsSection(title = "Password Reset", subtitle = "Set a new account password") {} }
        item {
            OutlinedTextField(value = newPassword, onValueChange = { newPassword = it }, label = { Text("New Password") }, visualTransformation = PasswordVisualTransformation(), modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 16.dp, vertical = 8.dp))
        }
        item {
            OutlinedTextField(value = confirmPassword, onValueChange = { confirmPassword = it }, label = { Text("Confirm Password") }, visualTransformation = PasswordVisualTransformation(), modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 16.dp, vertical = 8.dp))
        }
        item {
            Row(modifier = Modifier.fillMaxWidth().padding(16.dp), horizontalArrangement = Arrangement.End) {
                Button(onClick = { if (newPassword.length >= 8 && newPassword == confirmPassword) vm.resetPassword(newPassword) }, enabled = newPassword.length >= 8 && newPassword == confirmPassword) { Text("Set New Password") }
            }
        }

        if (message.isNotEmpty()) item { Text(message, modifier = Modifier.padding(16.dp), color = MaterialTheme.colorScheme.primary) }
        if (error.isNotEmpty()) item { Text(error, modifier = Modifier.padding(16.dp), color = MaterialTheme.colorScheme.error) }
    }
}