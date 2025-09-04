package xyz.sonet.app.theme

import android.app.WallpaperManager
import android.content.Context
import android.graphics.Bitmap
import android.graphics.Color
import android.graphics.drawable.Drawable
import android.os.Build
import androidx.annotation.RequiresApi
import androidx.core.graphics.drawable.toBitmap
import androidx.palette.graphics.Palette
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

@RequiresApi(Build.VERSION_CODES.S)
class MaterialYouThemeManager(private val context: Context) {
    
    private val wallpaperManager = WallpaperManager.getInstance(context)
    
    data class MaterialYouColors(
        val primary: Int,
        val onPrimary: Int,
        val primaryContainer: Int,
        val onPrimaryContainer: Int,
        val secondary: Int,
        val onSecondary: Int,
        val secondaryContainer: Int,
        val onSecondaryContainer: Int,
        val tertiary: Int,
        val onTertiary: Int,
        val tertiaryContainer: Int,
        val onTertiaryContainer: Int,
        val surface: Int,
        val onSurface: Int,
        val surfaceVariant: Int,
        val onSurfaceVariant: Int,
        val background: Int,
        val onBackground: Int,
        val error: Int,
        val onError: Int,
        val errorContainer: Int,
        val onErrorContainer: Int,
        val outline: Int,
        val outlineVariant: Int,
        val inverseSurface: Int,
        val inverseOnSurface: Int,
        val inversePrimary: Int,
        val shadow: Int,
        val scrim: Int
    )
    
    suspend fun extractColorsFromWallpaper(): MaterialYouColors = withContext(Dispatchers.IO) {
        try {
            val wallpaper = wallpaperManager.drawable
            val bitmap = wallpaper.toBitmap()
            val palette = Palette.from(bitmap).generate()
            
            extractMaterialYouColors(palette)
        } catch (e: Exception) {
            // Fallback to default Material 3 colors
            getDefaultMaterialYouColors()
        }
    }
    
    private fun extractMaterialYouColors(palette: Palette): MaterialYouColors {
        val dominantColor = palette.dominantColor ?: Color.BLUE
        val vibrantColor = palette.vibrantColor ?: dominantColor
        val mutedColor = palette.mutedColor ?: dominantColor
        
        // Generate Material You color scheme based on wallpaper
        val primary = vibrantColor
        val onPrimary = getContrastColor(primary)
        val primaryContainer = adjustColor(primary, 0.1f)
        val onPrimaryContainer = getContrastColor(primaryContainer)
        
        val secondary = mutedColor
        val onSecondary = getContrastColor(secondary)
        val secondaryContainer = adjustColor(secondary, 0.1f)
        val onSecondaryContainer = getContrastColor(secondaryContainer)
        
        val tertiary = adjustColor(primary, 0.3f)
        val onTertiary = getContrastColor(tertiary)
        val tertiaryContainer = adjustColor(tertiary, 0.1f)
        val onTertiaryContainer = getContrastColor(tertiaryContainer)
        
        val surface = Color.WHITE
        val onSurface = Color.BLACK
        val surfaceVariant = adjustColor(surface, -0.05f)
        val onSurfaceVariant = adjustColor(onSurface, 0.3f)
        
        val background = Color.WHITE
        val onBackground = Color.BLACK
        val error = Color.RED
        val onError = Color.WHITE
        val errorContainer = adjustColor(error, 0.1f)
        val onErrorContainer = getContrastColor(errorContainer)
        
        val outline = adjustColor(onSurface, 0.5f)
        val outlineVariant = adjustColor(outline, 0.3f)
        val inverseSurface = onSurface
        val inverseOnSurface = surface
        val inversePrimary = primary
        val shadow = Color.BLACK
        val scrim = Color.BLACK
        
        return MaterialYouColors(
            primary, onPrimary, primaryContainer, onPrimaryContainer,
            secondary, onSecondary, secondaryContainer, onSecondaryContainer,
            tertiary, onTertiary, tertiaryContainer, onTertiaryContainer,
            surface, onSurface, surfaceVariant, onSurfaceVariant,
            background, onBackground, error, onError, errorContainer, onErrorContainer,
            outline, outlineVariant, inverseSurface, inverseOnSurface,
            inversePrimary, shadow, scrim
        )
    }
    
    private fun getDefaultMaterialYouColors(): MaterialYouColors {
        return MaterialYouColors(
            primary = Color.parseColor("#6750A4"),
            onPrimary = Color.WHITE,
            primaryContainer = Color.parseColor("#EADDFF"),
            onPrimaryContainer = Color.parseColor("#21005D"),
            secondary = Color.parseColor("#625B71"),
            onSecondary = Color.WHITE,
            secondaryContainer = Color.parseColor("#E8DEF8"),
            onSecondaryContainer = Color.parseColor("#1D192B"),
            tertiary = Color.parseColor("#7D5260"),
            onTertiary = Color.WHITE,
            tertiaryContainer = Color.parseColor("#FFD8E4"),
            onTertiaryContainer = Color.parseColor("#31111D"),
            surface = Color.WHITE,
            onSurface = Color.parseColor("#1C1B1F"),
            surfaceVariant = Color.parseColor("#E7E0EC"),
            onSurfaceVariant = Color.parseColor("#49454F"),
            background = Color.WHITE,
            onBackground = Color.parseColor("#1C1B1F"),
            error = Color.parseColor("#B3261E"),
            onError = Color.WHITE,
            errorContainer = Color.parseColor("#F9DEDC"),
            onErrorContainer = Color.parseColor("#410E0B"),
            outline = Color.parseColor("#79747E"),
            outlineVariant = Color.parseColor("#CAC4D0"),
            inverseSurface = Color.parseColor("#1C1B1F"),
            inverseOnSurface = Color.WHITE,
            inversePrimary = Color.parseColor("#D0BCFF"),
            shadow = Color.BLACK,
            scrim = Color.BLACK
        )
    }
    
    private fun getContrastColor(color: Int): Int {
        val luminance = (0.299 * Color.red(color) + 0.587 * Color.green(color) + 0.114 * Color.blue(color)) / 255
        return if (luminance > 0.5) Color.BLACK else Color.WHITE
    }
    
    private fun adjustColor(color: Int, factor: Float): Int {
        val red = (Color.red(color) * (1 + factor)).coerceIn(0f, 255f).toInt()
        val green = (Color.green(color) * (1 + factor)).coerceIn(0f, 255f).toInt()
        val blue = (Color.blue(color) * (1 + factor)).coerceIn(0f, 255f).toInt()
        return Color.rgb(red, green, blue)
    }
    
    fun isMaterialYouSupported(): Boolean {
        return Build.VERSION.SDK_INT >= Build.VERSION_CODES.S
    }
    
    fun getWallpaperColors(): WallpaperColors? {
        return if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            wallpaperManager.wallpaperColors
        } else null
    }
}