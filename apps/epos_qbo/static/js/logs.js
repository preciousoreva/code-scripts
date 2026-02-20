// Logs page auto-refresh on run lifecycle events.
(function () {
    'use strict';

    const LOGS_REFRESH_DEBOUNCE_MS = 2000;
    const LOGS_COMPLETION_RETRY_DELAYS_MS = [3000, 9000, 18000];

    let listenersBound = false;

    function refreshLogsPage() {
        window.location.reload();
    }

    function bindLogsRefresh() {
        if (listenersBound) return;

        const runReactivity = window.OiatRunReactivity;
        if (runReactivity && typeof runReactivity.bindRunLifecycleRefresh === 'function') {
            runReactivity.bindRunLifecycleRefresh({
                onRefresh: refreshLogsPage,
                startedDelayMs: LOGS_REFRESH_DEBOUNCE_MS,
                completionDelaysMs: LOGS_COMPLETION_RETRY_DELAYS_MS,
            });
            listenersBound = true;
            return;
        }

        window.addEventListener('oiat:run-completed', refreshLogsPage);
        window.addEventListener('oiat:run-started', () => {
            window.setTimeout(refreshLogsPage, LOGS_REFRESH_DEBOUNCE_MS);
        });
        listenersBound = true;
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', bindLogsRefresh);
    } else {
        bindLogsRefresh();
    }
})();
