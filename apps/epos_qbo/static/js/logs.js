// Logs page auto-refresh on run completion
(function () {
    'use strict';

    const LOGS_REFRESH_DEBOUNCE_MS = 2000; // 2 seconds debounce
    const LOGS_REFRESH_AFTER_COMPLETION_MS = 3000; // 3 seconds after completion

    let refreshTimerId = null;
    let completionListenerBound = false;

    function refreshLogsPage() {
        // Reload the page to get fresh log events and stats
        // This preserves any active filters via URL params
        window.location.reload();
    }

    function scheduleLogsRefresh(delayMs) {
        const delay = delayMs !== undefined ? delayMs : LOGS_REFRESH_DEBOUNCE_MS;
        if (refreshTimerId) {
            clearTimeout(refreshTimerId);
        }
        refreshTimerId = window.setTimeout(() => {
            refreshLogsPage();
            refreshTimerId = null;
        }, delay);
    }

    function bindCompletionRefresh() {
        if (completionListenerBound) return;
        
        window.addEventListener('oiat:run-completed', (event) => {
            // Always refresh on completion - logs page shows all events
            scheduleLogsRefresh(LOGS_REFRESH_AFTER_COMPLETION_MS);
        });
        
        window.addEventListener('oiat:run-started', (event) => {
            // Refresh when a run starts to show new event
            scheduleLogsRefresh(LOGS_REFRESH_DEBOUNCE_MS);
        });
        
        completionListenerBound = true;
    }

    // Initialize on DOM ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', bindCompletionRefresh);
    } else {
        bindCompletionRefresh();
    }
})();
