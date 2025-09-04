package xyz.sonet.app.ui.components

import androidx.compose.foundation.layout.size
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp

@Composable
fun SonetLogo(
    size: LogoSize = LogoSize.MEDIUM,
    color: Color = MaterialTheme.colorScheme.onSurface
) {
    Text(
        text = "Sonet",
        fontSize = size.fontSize,
        fontWeight = size.fontWeight,
        color = color,
        letterSpacing = 1.2.sp,
        style = MaterialTheme.typography.headlineMedium
    )
}

enum class LogoSize(
    val fontSize: Int,
    val fontWeight: FontWeight
) {
    SMALL(20, FontWeight.SemiBold),
    MEDIUM(28, FontWeight.Bold),
    LARGE(36, FontWeight.ExtraBold)
}