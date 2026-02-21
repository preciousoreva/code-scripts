// Schedules page auto-refresh on run lifecycle events.
(function () {
    'use strict';

    const SCHEDULES_REFRESH_DEBOUNCE_MS = 1800;
    const SCHEDULES_COMPLETION_RETRY_DELAYS_MS = [2500, 7000, 15000];

    let listenersBound = false;

    function refreshSchedulesPage() {
        window.location.reload();
    }

    function bindSchedulesRefresh() {
        if (listenersBound) return;

        const runReactivity = window.OiatRunReactivity;
        if (runReactivity && typeof runReactivity.bindRunLifecycleRefresh === 'function') {
            runReactivity.bindRunLifecycleRefresh({
                onRefresh: refreshSchedulesPage,
                startedDelayMs: SCHEDULES_REFRESH_DEBOUNCE_MS,
                completionDelaysMs: SCHEDULES_COMPLETION_RETRY_DELAYS_MS,
            });
            listenersBound = true;
            return;
        }

        window.addEventListener('oiat:run-completed', refreshSchedulesPage);
        window.addEventListener('oiat:run-started', () => {
            window.setTimeout(refreshSchedulesPage, SCHEDULES_REFRESH_DEBOUNCE_MS);
        });
        listenersBound = true;
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', bindSchedulesRefresh);
    } else {
        bindSchedulesRefresh();
    }
})();
