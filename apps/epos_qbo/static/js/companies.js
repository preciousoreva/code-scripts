// Companies page auto-refresh on run completion
(function () {
    'use strict';

    const COMPANIES_REFRESH_DEBOUNCE_MS = 2000; // 2 seconds debounce
    const COMPANIES_REFRESH_AFTER_COMPLETION_MS = 3000; // 3 seconds after completion

    let refreshTimerId = null;
    let completionListenerBound = false;

    function refreshCompaniesList() {
        const companyList = document.getElementById('company-list');
        if (!companyList) return;

        // Get current filter/search/sort state
        const searchInput = document.querySelector('input[name="search"]');
        const filterSelect = document.querySelector('select[name="filter"]');
        const sortSelect = document.querySelector('select[name="sort"]');
        
        const search = searchInput ? searchInput.value : '';
        const filter = filterSelect ? filterSelect.value : 'all';
        const sort = sortSelect ? sortSelect.value : 'name';

        // Build URL with current state
        const params = new URLSearchParams();
        if (search) params.set('search', search);
        if (filter !== 'all') params.set('filter', filter);
        if (sort !== 'name') params.set('sort', sort);
        params.set('view', 'cards'); // Ensure cards view
        
        const url = `/epos-qbo/companies/?${params.toString()}`;

        // Fetch full page HTML (not HTMX partial) to get both summary and list
        fetch(url, {
            credentials: 'same-origin',
            headers: {
                Accept: 'text/html',
            },
        })
            .then((response) => {
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}`);
                }
                return response.text();
            })
            .then((html) => {
                // Parse HTML to extract summary cards and company list
                const parser = new DOMParser();
                const doc = parser.parseFromString(html, 'text/html');
                const summaryCards = doc.querySelector('.grid.grid-cols-2.md\\:grid-cols-4');
                const newCompanyList = doc.querySelector('#company-list');
                
                // Update summary cards if found
                if (summaryCards) {
                    const currentSummary = document.querySelector('.grid.grid-cols-2.md\\:grid-cols-4');
                    if (currentSummary && currentSummary.parentNode) {
                        currentSummary.parentNode.replaceChild(
                            document.importNode(summaryCards, true),
                            currentSummary
                        );
                    }
                }
                
                // Update company list
                if (newCompanyList) {
                    companyList.innerHTML = newCompanyList.innerHTML;
                }
            })
            .catch((err) => {
                console.error('Error refreshing companies page:', err);
            });
    }

    function scheduleCompaniesRefresh(delayMs) {
        const delay = delayMs !== undefined ? delayMs : COMPANIES_REFRESH_DEBOUNCE_MS;
        if (refreshTimerId) {
            clearTimeout(refreshTimerId);
        }
        refreshTimerId = window.setTimeout(() => {
            refreshCompaniesList();
            refreshTimerId = null;
        }, delay);
    }

    function bindCompletionRefresh() {
        if (completionListenerBound) return;
        
        window.addEventListener('oiat:run-completed', () => {
            scheduleCompaniesRefresh(COMPANIES_REFRESH_AFTER_COMPLETION_MS);
        });
        
        window.addEventListener('oiat:run-started', () => {
            scheduleCompaniesRefresh(COMPANIES_REFRESH_DEBOUNCE_MS);
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
