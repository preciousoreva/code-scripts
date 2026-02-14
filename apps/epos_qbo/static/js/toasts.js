// Toast Notification System
(function () {
    'use strict';

    const TOAST_DURATION = 5000; // 5 seconds
    const POLL_INTERVAL = 3000; // 3 seconds
    const ACTIVE_RUNS_URL = '/epos-qbo/api/runs/active';
    const RUN_STATUS_URL_BASE = '/epos-qbo/api/runs/status?job_ids=';

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
            if (this.activeRunIds.length === 0) {
                this.fetchGlobalActiveRunIds()
                    .then((ids) => {
                        this.mergeActiveRunIds(ids);
                    })
                    .finally(() => {
                        this.startPollingLoop();
                    });
                return;
            }
            this.startPollingLoop();
        },

        startPollingLoop() {
            if (this.activeRunIds.length === 0) {
                return;
            }
            this.activeRunIds.forEach((id) => {
                if (!this.previousStatuses.has(id)) {
                    this.previousStatuses.set(id, null);
                }
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
                const statusUrl = RUN_STATUS_URL_BASE + encodeURIComponent(jobIdsParam);
                fetch(statusUrl, {
                    credentials: 'same-origin',
                    headers: {
                        'Accept': 'application/json',
                    }
                })
                    .then(res => res.json())
                    .then(data => {
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
                                    this.show('success', 'Run Completed',
                                        `Run ${shortId}... completed successfully`,
                                        {
                                            id: `run-${jobId}`,
                                            link: '/epos-qbo/runs/' + jobId + '/'
                                        });
                                } else {
                                    const reason = statusInfo.failure_reason || 'Unknown error';
                                    const truncatedReason = reason.length > 50 ? reason.substring(0, 50) + '...' : reason;
                                    this.show('error', 'Run Failed',
                                        `Run ${shortId}... failed: ${truncatedReason}`,
                                        {
                                            id: `run-${jobId}`,
                                            link: '/epos-qbo/runs/' + jobId + '/'
                                        });
                                }
                                window.dispatchEvent(new CustomEvent('oiat:run-completed', {
                                    detail: {
                                        jobId: jobId,
                                        status: currentStatus,
                                        failureReason: statusInfo.failure_reason || null,
                                    }
                                }));

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

        fetchGlobalActiveRunIds() {
            return fetch(ACTIVE_RUNS_URL, {
                credentials: 'same-origin',
                headers: {
                    'Accept': 'application/json',
                }
            })
                .then((res) => {
                    if (!res.ok) {
                        return [];
                    }
                    return res.json();
                })
                .then((payload) => {
                    if (!payload || !Array.isArray(payload.job_ids)) {
                        return [];
                    }
                    return payload.job_ids.filter((value) => typeof value === 'string' && value.length > 0);
                })
                .catch(() => []);
        },

        mergeActiveRunIds(ids) {
            if (!Array.isArray(ids) || ids.length === 0) {
                return;
            }
            ids.forEach((id) => {
                if (!this.activeRunIds.includes(id)) {
                    this.activeRunIds.push(id);
                }
                if (!this.previousStatuses.has(id)) {
                    this.previousStatuses.set(id, null);
                }
            });
        },

        getActiveRunIds() {
            const runIds = [];

            // Check all data-active-runs sources on page
            const sources = Array.from(document.querySelectorAll('[data-active-runs]'));
            sources.forEach((node) => {
                const value = node.getAttribute('data-active-runs');
                if (!value) {
                    return;
                }
                try {
                    const ids = JSON.parse(value);
                    if (Array.isArray(ids)) {
                        runIds.push(...ids);
                    }
                } catch (e) {
                    console.error('Error parsing active runs:', e);
                }
            });

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
