package xyz.sonet.app.notifications

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import androidx.core.app.RemoteInput
import xyz.sonet.app.viewmodels.MessagingViewModel

class MessagingReplyReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent) {
        val input = RemoteInput.getResultsFromIntent(intent)
        val reply = input?.getCharSequence(MessagingNotificationHelper.EXTRA_REMOTE_INPUT)?.toString()?.trim()
        val conversationId = intent.getStringExtra(MessagingNotificationHelper.EXTRA_CONVERSATION_ID) ?: return
        if (!reply.isNullOrEmpty()) {
            // In a full implementation, inject a repository/service. For now, a simple service call.
            try {
                // TODO: route to background worker or service; placeholder noop
            } catch (_: Exception) {}
        }
    }
}

