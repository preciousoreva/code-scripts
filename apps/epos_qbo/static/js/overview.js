(function () {
    'use strict';

    function initCompanyFilter() {
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

    function initRevenueChart() {
        const canvas = document.getElementById('overview-revenue-chart');
        const dataScript = document.getElementById('overview-revenue-chart-data');
        if (!canvas || !dataScript || typeof Chart === 'undefined') return;

        let payload = null;
        try {
            payload = JSON.parse(dataScript.textContent || '{}');
        } catch (e) {
            return;
        }
        const labels = Array.isArray(payload.labels) ? payload.labels : [];
        const series = Array.isArray(payload.series) ? payload.series : [];
        if (!labels.length || !series.length) return;

        const datasets = series.map((item) => {
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

        const ctx = canvas.getContext('2d');
        if (!ctx) return;
        new Chart(ctx, {
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
                        grid: { display: false },
                        ticks: { maxRotation: 0, autoSkip: true },
                    },
                    y: {
                        beginAtZero: true,
                        ticks: {
                            callback(value) {
                                return formatCurrency(value);
                            },
                        },
                    },
                },
            },
        });
    }

    function initOverview() {
        initCompanyFilter();
        initRevenueChart();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initOverview);
    } else {
        initOverview();
    }
})();
