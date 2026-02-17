// Runs page auto-refresh on run lifecycle events.
(function () {
    'use strict';

    const RUNS_REFRESH_DEBOUNCE_MS = 2000;
    const RUNS_COMPLETION_RETRY_DELAYS_MS = [3000, 9000, 18000];

    let listenersBound = false;

    function refreshRunsPage() {
        window.location.reload();
    }

    function bindRunsRefresh() {
        if (listenersBound) return;

        const runReactivity = window.OiatRunReactivity;
        if (runReactivity && typeof runReactivity.bindRunLifecycleRefresh === 'function') {
            runReactivity.bindRunLifecycleRefresh({
                onRefresh: refreshRunsPage,
                startedDelayMs: RUNS_REFRESH_DEBOUNCE_MS,
                completionDelaysMs: RUNS_COMPLETION_RETRY_DELAYS_MS,
            });
            listenersBound = true;
            return;
        }

        window.addEventListener('oiat:run-completed', refreshRunsPage);
        window.addEventListener('oiat:run-started', () => {
            window.setTimeout(refreshRunsPage, RUNS_REFRESH_DEBOUNCE_MS);
        });
        listenersBound = true;
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', bindRunsRefresh);
    } else {
        bindRunsRefresh();
    }
})();
