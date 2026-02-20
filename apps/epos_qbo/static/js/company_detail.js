// Company detail page auto-refresh on run lifecycle events.
(function () {
    'use strict';

    const COMPANY_DETAIL_REFRESH_DEBOUNCE_MS = 2000;
    const COMPANY_DETAIL_COMPLETION_RETRY_DELAYS_MS = [3000, 9000, 18000];

    let listenersBound = false;

    function refreshCompanyDetail() {
        window.location.reload();
    }

    function bindCompanyDetailRefresh() {
        if (listenersBound) return;

        const runReactivity = window.OiatRunReactivity;
        if (runReactivity && typeof runReactivity.bindRunLifecycleRefresh === 'function') {
            runReactivity.bindRunLifecycleRefresh({
                onRefresh: refreshCompanyDetail,
                startedDelayMs: COMPANY_DETAIL_REFRESH_DEBOUNCE_MS,
                completionDelaysMs: COMPANY_DETAIL_COMPLETION_RETRY_DELAYS_MS,
            });
            listenersBound = true;
            return;
        }

        window.addEventListener('oiat:run-completed', refreshCompanyDetail);
        window.addEventListener('oiat:run-started', () => {
            window.setTimeout(refreshCompanyDetail, COMPANY_DETAIL_REFRESH_DEBOUNCE_MS);
        });
        listenersBound = true;
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', bindCompanyDetailRefresh);
    } else {
        bindCompanyDetailRefresh();
    }
})();
