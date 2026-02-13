// Toast Notification System
(function () {
    'use strict';

    const TOAST_DURATION = 5000; // 5 seconds
    const POLL_INTERVAL = 3000; // 3 seconds
    const DEBUG_LOG_ENDPOINT = 'http://localhost:7245/ingest/d47de936-96f2-4401-b426-fc69dd32d832';
    const ENABLE_DEBUG_BEACON = document.body?.dataset?.debugBeacon === '1';

    function debugLog(location, message, data, hypothesisId) {
        if (!ENABLE_DEBUG_BEACON) return;
        try {
            fetch(DEBUG_LOG_ENDPOINT, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ location, message, data: data || {}, timestamp: Date.now(), hypothesisId }) }).catch(function () { });
        } catch (e) { }
    }

    // Toast management
    const ToastManager = {
        container: null,
        shownToasts: new Set(),
        pollingInterval: null,
        activeRunIds: [],
        previousStatuses: new Map(),

        init() {
            this.container = document.getElementById('toast-container');
            if (!this.container) {
                console.warn('Toast container not found');
                return;
            }
            // #region agent log
            var runIds = this.getActiveRunIds();
            debugLog('toasts.js:init', 'ToastManager init', { activeRunIds: runIds, runIdsLength: runIds.length }, 'H6');
            // #endregion
            // Initialize run status polling if active runs exist
            this.initRunStatusPolling();
        },

        show(type, title, message, options = {}) {
            if (!this.container) return;

            const toastId = options.id || `${type}-${Date.now()}-${Math.random()}`;

            // Prevent duplicate toasts
            if (options.id && this.shownToasts.has(toastId)) {
                return;
            }
            this.shownToasts.add(toastId);

            const toast = document.createElement('div');
            toast.className = `toast toast-${type}`;
            toast.dataset.toastId = toastId;

            const iconMap = {
                success: 'solar:check-circle-linear',
                error: 'solar:close-circle-linear',
                info: 'solar:info-circle-linear'
            };

            const icon = iconMap[type] || 'solar:info-circle-linear';

            toast.innerHTML = `
                <iconify-icon icon="${icon}" width="20" class="toast-icon"></iconify-icon>
                <div class="toast-content">
                    <div class="toast-title">${this.escapeHtml(title)}</div>
                    <div class="toast-message">${this.escapeHtml(message)}</div>
                </div>
                <button class="toast-close" onclick="ToastManager.remove('${toastId}')" aria-label="Close">
                    <iconify-icon icon="solar:close-circle-linear" width="16"></iconify-icon>
                </button>
            `;

            // Add click handler if link provided
            if (options.link) {
                toast.style.cursor = 'pointer';
                toast.addEventListener('click', () => {
                    window.location.href = options.link;
                });
            }

            this.container.appendChild(toast);

            // Auto-dismiss
            const duration = options.duration !== undefined ? options.duration : TOAST_DURATION;
            if (duration > 0) {
                setTimeout(() => {
                    this.remove(toastId);
                }, duration);
            }
        },

        remove(toastId) {
            const toast = this.container?.querySelector(`[data-toast-id="${toastId}"]`);
            if (toast) {
                toast.classList.add('hiding');
                setTimeout(() => {
                    toast.remove();
                    this.shownToasts.delete(toastId);
                }, 300);
            }
        },

        escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        },

        // Run status polling
        initRunStatusPolling() {
            // Get active run IDs from page
            this.activeRunIds = this.getActiveRunIds();
            if (this.activeRunIds.length === 0) return;

            // Initialize previous statuses
            this.activeRunIds.forEach(id => {
                this.previousStatuses.set(id, null);
            });

            const poll = () => {
                if (this.activeRunIds.length === 0) {
                    if (this.pollingInterval) {
                        clearInterval(this.pollingInterval);
                        this.pollingInterval = null;
                    }
                    return;
                }

                const jobIdsParam = this.activeRunIds.join(',');
                const statusUrl = '/epos-qbo/api/runs/status?job_ids=' + jobIdsParam;
                // #region agent log
                debugLog('toasts.js:poll_request', 'Polling run status', { url: statusUrl, jobIdsParam: jobIdsParam.substring(0, 100) }, 'H7');
                // #endregion
                fetch(statusUrl, {
                    credentials: 'same-origin',
                    headers: {
                        'Accept': 'application/json',
                    }
                })
                    .then(res => res.json())
                    .then(data => {
                        // #region agent log
                        debugLog('toasts.js:poll_response', 'Poll response', { keys: Object.keys(data || {}), statuses: data ? Object.fromEntries(Object.entries(data).map(function (e) { return [e[0], e[1].status]; })) : {} }, 'H7');
                        // #endregion
                        this.activeRunIds.forEach(jobId => {
                            const statusInfo = data[jobId];
                            if (!statusInfo) return;

                            const prevStatus = this.previousStatuses.get(jobId);
                            const currentStatus = statusInfo.status;

                            // Detect status change to completed states
                            if (prevStatus &&
                                (prevStatus === 'queued' || prevStatus === 'running') &&
                                (currentStatus === 'succeeded' || currentStatus === 'failed')) {

                                // Show toast notification
                                const shortId = jobId.substring(0, 8);
                                if (currentStatus === 'succeeded') {
                                    // #region agent log
                                    debugLog('toasts.js:toast_show', 'Toast: Run completed', { jobId: jobId.substring(0, 8), status: currentStatus }, 'H8');
                                    // #endregion
                                    this.show('success', 'Run Completed',
                                        `Run ${shortId}... completed successfully`,
                                        {
                                            id: `run-${jobId}`,
                                            link: '/epos-qbo/runs/' + jobId + '/'
                                        });
                                } else {
                                    // #region agent log
                                    debugLog('toasts.js:toast_show', 'Toast: Run failed', { jobId: jobId.substring(0, 8), status: currentStatus }, 'H8');
                                    // #endregion
                                    const reason = statusInfo.failure_reason || 'Unknown error';
                                    const truncatedReason = reason.length > 50 ? reason.substring(0, 50) + '...' : reason;
                                    this.show('error', 'Run Failed',
                                        `Run ${shortId}... failed: ${truncatedReason}`,
                                        {
                                            id: `run-${jobId}`,
                                            link: '/epos-qbo/runs/' + jobId + '/'
                                        });
                                }

                                // Remove from polling list
                                const index = this.activeRunIds.indexOf(jobId);
                                if (index > -1) {
                                    this.activeRunIds.splice(index, 1);
                                }
                            }

                            this.previousStatuses.set(jobId, currentStatus);
                        });
                    })
                    .catch(err => {
                        // #region agent log
                        debugLog('toasts.js:poll_error', 'Poll failed', { err: String(err && err.message || err) }, 'H7');
                        // #endregion
                        console.error('Error polling run status:', err);
                    });
            };

            // Start polling
            poll();
            this.pollingInterval = setInterval(poll, POLL_INTERVAL);

            // Stop polling when page unloads
            window.addEventListener('beforeunload', () => {
                if (this.pollingInterval) {
                    clearInterval(this.pollingInterval);
                }
            });
        },

        getActiveRunIds() {
            const runIds = [];

            // Check for data attribute on body or main element
            const bodyData = document.body.dataset.activeRuns;
            const mainData = document.querySelector('main')?.dataset.activeRuns;
            const activeRunsData = bodyData || mainData;

            if (activeRunsData) {
                try {
                    const ids = JSON.parse(activeRunsData);
                    if (Array.isArray(ids)) {
                        runIds.push(...ids);
                    }
                } catch (e) {
                    console.error('Error parsing active runs:', e);
                }
            }

            // Also check for run detail page
            const runDetailId = document.querySelector('[data-run-id]')?.dataset.runId;
            if (runDetailId) {
                runIds.push(runDetailId);
            }

            return [...new Set(runIds)]; // Remove duplicates
        }
    };

    // Expose globally
    window.ToastManager = ToastManager;

    // Initialize on DOM ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', () => ToastManager.init());
    } else {
        ToastManager.init();
    }
})();
