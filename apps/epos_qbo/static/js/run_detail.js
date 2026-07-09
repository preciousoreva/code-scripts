// Run detail page auto-refresh while the current run transitions state.
(function () {
    'use strict';

    const RUN_DETAIL_REFRESH_DEBOUNCE_MS = 1500;
    const RUN_DETAIL_COMPLETION_RETRY_DELAYS_MS = [2000, 6000, 12000, 20000];
    const ACTIVE_STATUSES = new Set(['queued', 'running']);

    let listenersBound = false;

    function runDetailRoot() {
        return document.querySelector('[data-run-id]');
    }

    function isRunStillActive() {
        const root = runDetailRoot();
        if (!root) return false;
        const status = (root.dataset.runStatus || '').trim().toLowerCase();
        return ACTIVE_STATUSES.has(status);
    }

    function refreshRunDetailPage() {
        if (!runDetailRoot()) return;
        window.location.reload();
    }

    function shouldRefreshForCurrentRun(_phase, detail) {
        const root = runDetailRoot();
        if (!root) return false;
        const currentRunId = (root.dataset.runId || '').trim();
        if (!currentRunId) return false;

        const eventRunId = detail && typeof detail.jobId === 'string' ? detail.jobId : '';
        if (eventRunId) {
            return eventRunId === currentRunId;
        }

        const eventRunIds = detail && Array.isArray(detail.jobIds) ? detail.jobIds : [];
        if (eventRunIds.length > 0) {
            return eventRunIds.includes(currentRunId);
        }

        return true;
    }

    function bindRunDetailRefresh() {
        if (listenersBound || !isRunStillActive()) return;

        const runReactivity = window.OiatRunReactivity;
        if (runReactivity && typeof runReactivity.bindRunLifecycleRefresh === 'function') {
            runReactivity.bindRunLifecycleRefresh({
                onRefresh: refreshRunDetailPage,
                shouldRefresh: shouldRefreshForCurrentRun,
                startedDelayMs: RUN_DETAIL_REFRESH_DEBOUNCE_MS,
                completionDelaysMs: RUN_DETAIL_COMPLETION_RETRY_DELAYS_MS,
            });
            listenersBound = true;
            return;
        }

        window.addEventListener('oiat:run-completed', (event) => {
            if (shouldRefreshForCurrentRun('completed', event.detail || {})) {
                refreshRunDetailPage();
            }
        });
        window.addEventListener('oiat:run-started', (event) => {
            if (shouldRefreshForCurrentRun('started', event.detail || {})) {
                window.setTimeout(refreshRunDetailPage, RUN_DETAIL_REFRESH_DEBOUNCE_MS);
            }
        });
        listenersBound = true;
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', bindRunDetailRefresh);
    } else {
        bindRunDetailRefresh();
    }
})();
