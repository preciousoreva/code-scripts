// Shared run lifecycle reactivity for dashboard pages.
(function () {
    'use strict';

    const DEFAULT_STARTED_DELAY_MS = 1200;
    const DEFAULT_COMPLETION_DELAYS_MS = [2000, 6000, 12000, 20000];

    function normalizeDelays(delays, fallback) {
        if (!Array.isArray(delays) || delays.length === 0) {
            return fallback.slice();
        }
        const normalized = delays
            .map((value) => Number(value))
            .filter((value) => Number.isFinite(value) && value >= 0);
        return normalized.length ? normalized : fallback.slice();
    }

    function bindRunLifecycleRefresh(options = {}) {
        const onRefresh = typeof options.onRefresh === 'function' ? options.onRefresh : null;
        if (!onRefresh) {
            return { destroy() {} };
        }

        const startedDelayMs = Number.isFinite(Number(options.startedDelayMs))
            ? Number(options.startedDelayMs)
            : DEFAULT_STARTED_DELAY_MS;
        const completionDelaysMs = normalizeDelays(
            options.completionDelaysMs,
            DEFAULT_COMPLETION_DELAYS_MS
        );
        const shouldRefresh = typeof options.shouldRefresh === 'function'
            ? options.shouldRefresh
            : () => true;
        const checkFreshnessAfterRefresh = typeof options.checkFreshnessAfterRefresh === 'function'
            ? options.checkFreshnessAfterRefresh
            : null;

        let startedTimerId = null;
        let completionTimerIds = [];
        let refreshInFlight = false;
        let refreshQueued = false;
        let lastCompletedJobId = null;
        let currentTriggerCompletionJobId = null;

        function clearCompletionTimers() {
            if (!completionTimerIds.length) return;
            completionTimerIds.forEach((timerId) => clearTimeout(timerId));
            completionTimerIds = [];
        }

        function scheduleStartedRefresh() {
            if (startedTimerId) clearTimeout(startedTimerId);
            startedTimerId = window.setTimeout(() => {
                startedTimerId = null;
                currentTriggerCompletionJobId = null;
                triggerRefresh();
            }, startedDelayMs);
        }

        function scheduleCompletionRefreshes(event) {
            clearCompletionTimers();
            lastCompletedJobId = (event && event.detail && event.detail.jobId) ? event.detail.jobId : null;
            completionTimerIds = completionDelaysMs.map((delay) => (
                window.setTimeout(() => {
                    currentTriggerCompletionJobId = lastCompletedJobId;
                    triggerRefresh();
                }, delay)
            ));
        }

        function triggerRefresh() {
            if (refreshInFlight) {
                refreshQueued = true;
                return;
            }
            const completionJobId = currentTriggerCompletionJobId;
            refreshInFlight = true;
            Promise.resolve()
                .then(() => onRefresh())
                .catch(() => {
                    // Best effort; page should remain usable on transient failures.
                })
                .finally(() => {
                    refreshInFlight = false;
                    if (completionJobId && checkFreshnessAfterRefresh) {
                        Promise.resolve(checkFreshnessAfterRefresh(completionJobId)).then(function (fresh) {
                            if (fresh) {
                                clearCompletionTimers();
                            }
                        });
                    }
                    currentTriggerCompletionJobId = null;
                    if (refreshQueued) {
                        refreshQueued = false;
                        triggerRefresh();
                    }
                });
        }

        function onCompleted(event) {
            if (!shouldRefresh('completed', event && event.detail ? event.detail : {})) return;
            scheduleCompletionRefreshes(event);
        }

        function onStarted(event) {
            if (!shouldRefresh('started', event && event.detail ? event.detail : {})) return;
            scheduleStartedRefresh();
        }

        function onBeforeUnload() {
            if (startedTimerId) clearTimeout(startedTimerId);
            clearCompletionTimers();
        }

        window.addEventListener('oiat:run-completed', onCompleted);
        window.addEventListener('oiat:run-started', onStarted);
        window.addEventListener('beforeunload', onBeforeUnload);

        return {
            destroy() {
                window.removeEventListener('oiat:run-completed', onCompleted);
                window.removeEventListener('oiat:run-started', onStarted);
                window.removeEventListener('beforeunload', onBeforeUnload);
                onBeforeUnload();
            },
        };
    }

    window.OiatRunReactivity = {
        bindRunLifecycleRefresh,
        DEFAULT_STARTED_DELAY_MS,
        DEFAULT_COMPLETION_DELAYS_MS,
    };
})();
