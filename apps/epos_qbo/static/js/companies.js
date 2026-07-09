// Companies page refreshes summary/list on run lifecycle events.
(function () {
    'use strict';

    const COMPANIES_REFRESH_DEBOUNCE_MS = 2000;
    const COMPANIES_COMPLETION_RETRY_DELAYS_MS = [3000, 9000, 18000];

    let listenersBound = false;
    let refreshInFlight = false;
    let refreshQueued = false;

    function currentCompaniesUrl() {
        const searchInput = document.querySelector('input[name="search"]');
        const filterSelect = document.querySelector('select[name="filter"]');
        const sortSelect = document.querySelector('select[name="sort"]');

        const search = searchInput ? searchInput.value : '';
        const filter = filterSelect ? filterSelect.value : 'all';
        const sort = sortSelect ? sortSelect.value : 'name';

        const params = new URLSearchParams();
        if (search) params.set('search', search);
        if (filter !== 'all') params.set('filter', filter);
        if (sort !== 'name') params.set('sort', sort);
        params.set('view', 'cards');
        return `/epos-qbo/companies/?${params.toString()}`;
    }

    function refreshCompaniesList() {
        const companyList = document.getElementById('company-list');
        if (!companyList) return Promise.resolve();

        if (refreshInFlight) {
            refreshQueued = true;
            return Promise.resolve();
        }
        refreshInFlight = true;

        const url = currentCompaniesUrl();
        return fetch(url, {
            credentials: 'same-origin',
            headers: { Accept: 'text/html' },
        })
            .then((response) => {
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}`);
                }
                return response.text();
            })
            .then((html) => {
                const parser = new DOMParser();
                const doc = parser.parseFromString(html, 'text/html');

                const refreshedSummary = doc.querySelector('#companies-summary-cards');
                const currentSummary = document.querySelector('#companies-summary-cards');
                if (refreshedSummary && currentSummary && currentSummary.parentNode) {
                    currentSummary.parentNode.replaceChild(
                        document.importNode(refreshedSummary, true),
                        currentSummary
                    );
                }

                const refreshedList = doc.querySelector('#company-list');
                if (refreshedList) {
                    companyList.innerHTML = refreshedList.innerHTML;
                }

                const refreshedEmpty = doc.querySelector('#companies-empty-state');
                const currentEmpty = document.querySelector('#companies-empty-state');
                if (refreshedEmpty) {
                    const importedEmpty = document.importNode(refreshedEmpty, true);
                    if (currentEmpty && currentEmpty.parentNode) {
                        currentEmpty.parentNode.replaceChild(importedEmpty, currentEmpty);
                    } else if (companyList.parentNode) {
                        companyList.parentNode.insertBefore(importedEmpty, companyList.nextSibling);
                    }
                } else if (currentEmpty && currentEmpty.parentNode) {
                    currentEmpty.remove();
                }
            })
            .catch(() => {
                // Keep current content on transient refresh failures.
            })
            .finally(() => {
                refreshInFlight = false;
                if (refreshQueued) {
                    refreshQueued = false;
                    refreshCompaniesList();
                }
            });
    }

    function bindCompaniesRefresh() {
        if (listenersBound) return;

        const runReactivity = window.OiatRunReactivity;
        if (runReactivity && typeof runReactivity.bindRunLifecycleRefresh === 'function') {
            runReactivity.bindRunLifecycleRefresh({
                onRefresh: refreshCompaniesList,
                startedDelayMs: COMPANIES_REFRESH_DEBOUNCE_MS,
                completionDelaysMs: COMPANIES_COMPLETION_RETRY_DELAYS_MS,
            });
            listenersBound = true;
            return;
        }

        window.addEventListener('oiat:run-completed', refreshCompaniesList);
        window.addEventListener('oiat:run-started', () => {
            window.setTimeout(refreshCompaniesList, COMPANIES_REFRESH_DEBOUNCE_MS);
        });
        listenersBound = true;
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', bindCompaniesRefresh);
    } else {
        bindCompaniesRefresh();
    }
})();
