// Runs page auto-refresh on run completion
(function () {
    'use strict';

    const RUNS_REFRESH_DEBOUNCE_MS = 2000; // 2 seconds debounce
    const RUNS_REFRESH_AFTER_COMPLETION_MS = 3000; // 3 seconds after completion

    let refreshTimerId = null;
    let completionListenerBound = false;

    function refreshRunsTable() {
        // Reload the page to get fresh run data
        // This is simpler than trying to update the table rows individually
        window.location.reload();
    }

    function scheduleRunsRefresh(delayMs) {
        const delay = delayMs !== undefined ? delayMs : RUNS_REFRESH_DEBOUNCE_MS;
        if (refreshTimerId) {
            clearTimeout(refreshTimerId);
        }
        refreshTimerId = window.setTimeout(() => {
            refreshRunsTable();
            refreshTimerId = null;
        }, delay);
    }

    function bindCompletionRefresh() {
        if (completionListenerBound) return;
        
        window.addEventListener('oiat:run-completed', (event) => {
            // Always refresh on completion - runs page shows all runs
            scheduleRunsRefresh(RUNS_REFRESH_AFTER_COMPLETION_MS);
        });
        
        window.addEventListener('oiat:run-started', (event) => {
            // Refresh when a run starts to show it in the table
            scheduleRunsRefresh(RUNS_REFRESH_DEBOUNCE_MS);
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
