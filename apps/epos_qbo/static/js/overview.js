(function () {
    'use strict';
    const OVERVIEW_PANELS_URL = '/epos-qbo/dashboard/panels/';
    const OVERVIEW_REFRESH_DEBOUNCE_MS = 800;
    const OVERVIEW_REFRESH_AFTER_COMPLETION_MS = 2000;
    let revenueChart = null;
    let refreshTimerId = null;
    let completionListenerBound = false;

    function initCompanyFilter(initialQuery) {
        const input = document.getElementById('overview-company-filter');
        if (!input) return;

        const rows = Array.from(document.querySelectorAll('.overview-company-row'));
        const emptyState = document.getElementById('overview-company-filter-empty');
        if (rows.length === 0) return;

        const applyFilter = () => {
            const query = input.value.trim().toLowerCase();
            let visibleCount = 0;

            rows.forEach((row) => {
                const haystack = (row.dataset.search || '').toLowerCase();
                const matches = !query || haystack.includes(query);
                row.classList.toggle('hidden', !matches);
                if (matches) visibleCount += 1;
            });

            if (emptyState) {
                emptyState.classList.toggle('hidden', visibleCount > 0);
            }
        };

        if (typeof initialQuery === 'string') {
            input.value = initialQuery;
        }
        input.addEventListener('input', applyFilter);
        applyFilter();
    }

    function colorForCompany(companyKey) {
        const palette = [
            '#0f766e',
            '#1d4ed8',
            '#b45309',
            '#be123c',
            '#4338ca',
            '#0f766e',
            '#0e7490',
            '#4d7c0f',
        ];
        const key = String(companyKey || '');
        let hash = 0;
        for (let i = 0; i < key.length; i += 1) {
            hash = ((hash << 5) - hash) + key.charCodeAt(i);
            hash |= 0;
        }
        return palette[Math.abs(hash) % palette.length];
    }

    function formatCurrency(amount) {
        try {
            return new Intl.NumberFormat('en-NG', {
                style: 'currency',
                currency: 'NGN',
                maximumFractionDigits: 2,
            }).format(amount || 0);
        } catch (e) {
            return `NGN ${Number(amount || 0).toFixed(2)}`;
        }
    }

    function updateRevenueSummary(totalAmount, matchedDays) {
        const matchedDaysEl = document.getElementById('overview-revenue-matched-days');
        if (matchedDaysEl) {
            matchedDaysEl.textContent = `Matched days in period: ${matchedDays || 0}`;
        }
    }

    function bindRevenueCompanyFilter(selectedCompanyKey) {
        const select = document.getElementById('overview-revenue-company');
        if (!select) return 'all';
        const available = Array.from(select.options).map((opt) => opt.value);
        const normalized = selectedCompanyKey && available.includes(selectedCompanyKey) ? selectedCompanyKey : 'all';
        select.value = normalized;
        select.onchange = () => {
            initRevenueChart(select.value);
        };
        return select.value;
    }

    function initRevenueChart(selectedCompanyKey) {
        const canvas = document.getElementById('overview-revenue-chart');
        const dataScript = document.getElementById('overview-revenue-chart-data');
        const companyKey = bindRevenueCompanyFilter(selectedCompanyKey);
        if (!canvas || !dataScript || typeof Chart === 'undefined') {
            if (revenueChart) {
                revenueChart.destroy();
                revenueChart = null;
            }
            return;
        }

        if (revenueChart) {
            revenueChart.destroy();
            revenueChart = null;
        }

        let payload = null;
        try {
            payload = JSON.parse(dataScript.textContent || '{}');
        } catch (e) {
            return;
        }
        const labels = Array.isArray(payload.labels) ? payload.labels : [];
        const series = Array.isArray(payload.series) ? payload.series : [];
        if (!labels.length || !series.length) {
            updateRevenueSummary(0, 0);
            return;
        }

        const filteredSeries = companyKey === 'all'
            ? series
            : series.filter((item) => item.company_key === companyKey);
        if (!filteredSeries.length) {
            updateRevenueSummary(0, 0);
            return;
        }

        const datasets = filteredSeries.map((item) => {
            const stroke = colorForCompany(item.company_key || item.name);
            return {
                label: item.name || item.company_key || 'Company',
                data: Array.isArray(item.data) ? item.data : [],
                borderColor: stroke,
                backgroundColor: `${stroke}22`,
                borderWidth: 2,
                pointRadius: 2,
                pointHoverRadius: 4,
                tension: 0.25,
                fill: false,
            };
        });

        let totalAmount = 0;
        const dayTotals = Array(labels.length).fill(0);
        filteredSeries.forEach((item) => {
            const data = Array.isArray(item.data) ? item.data : [];
            data.forEach((value, idx) => {
                const amount = Number(value || 0);
                totalAmount += amount;
                if (idx < dayTotals.length) {
                    dayTotals[idx] += amount;
                }
            });
        });
        const matchedDays = dayTotals.reduce((count, amount) => (amount > 0 ? count + 1 : count), 0);
        updateRevenueSummary(totalAmount, matchedDays);

        const ctx = canvas.getContext('2d');
        if (!ctx) return;
        const colors = typeof window.getChartColors === 'function' ? window.getChartColors() : { textColor: '#64748b', gridColor: 'rgba(226, 232, 240, 0.5)' };
        revenueChart = new Chart(ctx, {
            type: 'line',
            data: { labels, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: {
                            boxWidth: 10,
                            boxHeight: 10,
                            usePointStyle: true,
                            pointStyle: 'circle',
                            font: { size: 11 },
                            color: colors.textColor,
                        },
                    },
                    tooltip: {
                        callbacks: {
                            label(context) {
                                const label = context.dataset.label || 'Company';
                                const value = context.parsed && typeof context.parsed.y === 'number' ? context.parsed.y : 0;
                                return `${label}: ${formatCurrency(value)}`;
                            },
                        },
                    },
                },
                scales: {
                    x: {
                        grid: { display: false, color: colors.gridColor },
                        ticks: { maxRotation: 0, autoSkip: true, color: colors.textColor },
                    },
                    y: {
                        beginAtZero: true,
                        grid: { color: colors.gridColor },
                        ticks: {
                            color: colors.textColor,
                            callback(value) {
                                return formatCurrency(value);
                            },
                        },
                    },
                },
            },
        });
    }

    function currentRevenuePeriod() {
        const select = document.getElementById('overview-revenue-period');
        if (!select) return '7d';
        const value = (select.value || '').trim().toLowerCase();
        if (['yesterday', '7d', '30d', '90d'].includes(value)) {
            return value;
        }
        return '7d';
    }

    function refreshOverviewPanels() {
        const root = document.getElementById('overview-panels-root');
        if (!root) return;
        const existingFilter = document.getElementById('overview-company-filter');
        const filterQuery = existingFilter ? existingFilter.value : '';
        const existingCompany = document.getElementById('overview-revenue-company');
        const revenueCompany = existingCompany ? existingCompany.value : 'all';
        const period = currentRevenuePeriod();
        const requestUrl = `${OVERVIEW_PANELS_URL}?revenue_period=${encodeURIComponent(period)}`;

        fetch(requestUrl, {
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
                root.innerHTML = html;
                initOverview({ filterQuery, revenueCompany });
            })
            .catch(() => {
                // Best-effort refresh: keep current panel content when request fails.
            });
    }

    function scheduleOverviewRefresh(delayMs) {
        const delay = delayMs !== undefined ? delayMs : OVERVIEW_REFRESH_DEBOUNCE_MS;
        if (refreshTimerId) {
            clearTimeout(refreshTimerId);
        }
        refreshTimerId = window.setTimeout(() => {
            refreshOverviewPanels();
            refreshTimerId = null;
        }, delay);
    }

    function bindCompletionRefresh() {
        if (completionListenerBound) return;
        window.addEventListener('oiat:run-completed', () => {
            scheduleOverviewRefresh(OVERVIEW_REFRESH_AFTER_COMPLETION_MS);
        });
        window.addEventListener('oiat:run-started', () => {
            scheduleOverviewRefresh(OVERVIEW_REFRESH_DEBOUNCE_MS);
        });
        completionListenerBound = true;
    }

    function bindRevenuePeriodChange() {
        const select = document.getElementById('overview-revenue-period');
        if (!select) return;
        select.onchange = () => {
            const period = currentRevenuePeriod();
            const url = new URL(window.location.href);
            url.searchParams.set('revenue_period', period);
            history.replaceState(null, '', url.pathname + (url.search || ''));
            refreshOverviewPanels();
        };
    }

    function bindThemeChange() {
        window.addEventListener('themeChange', () => {
            const companySelect = document.getElementById('overview-revenue-company');
            const company = companySelect ? companySelect.value : 'all';
            initRevenueChart(company);
        });
    }

    function initOverview(options = {}) {
        initCompanyFilter(options.filterQuery || '');
        initRevenueChart(options.revenueCompany || 'all');
        bindRevenuePeriodChange();
        bindCompletionRefresh();
        bindThemeChange();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initOverview);
    } else {
        initOverview();
    }
})();
