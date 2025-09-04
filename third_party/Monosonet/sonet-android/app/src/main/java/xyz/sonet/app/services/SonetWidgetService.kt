package xyz.sonet.app.services

import android.content.Intent
import android.widget.RemoteViewsService
import xyz.sonet.app.widgets.SonetWidgetRemoteViewsFactory

class SonetWidgetService : RemoteViewsService() {
    
    override fun onGetViewFactory(intent: Intent): RemoteViewsFactory {
        return SonetWidgetRemoteViewsFactory(applicationContext, intent)
    }
}