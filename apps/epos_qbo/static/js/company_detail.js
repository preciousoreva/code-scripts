// Company Detail page auto-refresh on run completion
(function () {
    'use strict';

    const COMPANY_DETAIL_REFRESH_DEBOUNCE_MS = 2000; // 2 seconds debounce
    const COMPANY_DETAIL_REFRESH_AFTER_COMPLETION_MS = 3000; // 3 seconds after completion

    let refreshTimerId = null;
    let completionListenerBound = false;

    // Extract company_key from URL (e.g., /epos-qbo/companies/company_a/)
    function getCurrentCompanyKey() {
        const pathParts = window.location.pathname.split('/');
        const companiesIndex = pathParts.indexOf('companies');
        if (companiesIndex >= 0 && companiesIndex < pathParts.length - 1) {
            return pathParts[companiesIndex + 1];
        }
        return null;
    }

    function refreshCompanyDetail() {
        // Reload the page to get fresh company data, recent runs, and metrics
        window.location.reload();
    }

    function scheduleCompanyDetailRefresh(delayMs) {
        const delay = delayMs !== undefined ? delayMs : COMPANY_DETAIL_REFRESH_DEBOUNCE_MS;
        if (refreshTimerId) {
            clearTimeout(refreshTimerId);
        }
        refreshTimerId = window.setTimeout(() => {
            refreshCompanyDetail();
            refreshTimerId = null;
        }, delay);
    }

    function bindCompletionRefresh() {
        if (completionListenerBound) return;
        
        const currentCompanyKey = getCurrentCompanyKey();
        
        window.addEventListener('oiat:run-completed', (event) => {
            // Refresh if this run affects this company (All Companies runs affect all)
            // For now, refresh on any completion - could be optimized to check job details
            scheduleCompanyDetailRefresh(COMPANY_DETAIL_REFRESH_AFTER_COMPLETION_MS);
        });
        
        window.addEventListener('oiat:run-started', (event) => {
            // Refresh when a run starts if it's for this company or All Companies
            scheduleCompanyDetailRefresh(COMPANY_DETAIL_REFRESH_DEBOUNCE_MS);
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
