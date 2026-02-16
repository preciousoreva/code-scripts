/**
 * Theme-aware chart utilities for the dashboard.
 * Use isDarkMode() and getChartColors() when building Chart.js options
 * so grid, ticks, and legend respect light/dark theme.
 * When the user changes theme (Settings → Appearance), the page dispatches
 * a custom "themeChange" event; listeners can re-create charts with new colors.
 */
(function () {
    'use strict';

    function isDarkMode() {
        return document.documentElement.classList.contains('dark');
    }

    function getChartColors() {
        const dark = isDarkMode();
        return {
            gridColor: dark ? 'rgba(71, 85, 105, 0.3)' : 'rgba(226, 232, 240, 0.5)',
            textColor: dark ? '#cbd5e1' : '#64748b',
            lineColors: [
                dark ? '#60a5fa' : '#3b82f6',
                dark ? '#34d399' : '#10b981',
                dark ? '#f59e0b' : '#d97706',
                dark ? '#f87171' : '#dc2626',
            ],
            backgroundColor: dark ? 'rgba(15, 23, 42, 0.8)' : 'rgba(255, 255, 255, 0.8)',
        };
    }

    if (typeof window !== 'undefined') {
        window.isDarkMode = isDarkMode;
        window.getChartColors = getChartColors;
    }
})();
