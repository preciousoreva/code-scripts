(function () {
    function getCookie(name) {
        var value = '; ' + document.cookie;
        var parts = value.split('; ' + name + '=');
        if (parts.length === 2) return parts.pop().split(';').shift();
        return '';
    }

    function text(value) {
        return String(value || '');
    }

    function appendMessage(container, role, body, sources, warnings) {
        var wrapper = document.createElement('div');
        var isUser = role === 'user';
        wrapper.className = isUser
            ? 'ml-8 rounded-md bg-blue-600 px-3 py-2 text-white'
            : 'mr-8 rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-slate-800 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-100';

        var message = document.createElement('div');
        message.className = 'whitespace-pre-wrap leading-relaxed';
        message.textContent = text(body);
        wrapper.appendChild(message);

        if (Array.isArray(warnings) && warnings.length) {
            var warningList = document.createElement('div');
            warningList.className = 'mt-2 text-xs text-amber-700 dark:text-amber-300';
            warningList.textContent = warnings.join(' ');
            wrapper.appendChild(warningList);
        }

        if (Array.isArray(sources) && sources.length) {
            var sourceList = document.createElement('div');
            sourceList.className = 'mt-2 flex flex-wrap gap-1.5';
            sources.forEach(function (source) {
                if (!source || !source.url || !source.label) return;
                var link = document.createElement('a');
                link.href = source.url;
                link.className = 'rounded border border-slate-200 bg-white px-2 py-0.5 text-xs text-slate-600 hover:text-blue-700 dark:border-slate-600 dark:bg-slate-900 dark:text-slate-300 dark:hover:text-blue-300';
                link.textContent = source.label;
                sourceList.appendChild(link);
            });
            wrapper.appendChild(sourceList);
        }

        container.appendChild(wrapper);
        container.scrollTop = container.scrollHeight;
        return wrapper;
    }

    function pageContext() {
        var path = window.location.pathname;
        var companyMatch = path.match(/\/companies\/([^/]+)/);
        var runMatch = path.match(/\/runs\/([0-9a-fA-F-]{36})/);
        return {
            path: path,
            company_key: companyMatch ? decodeURIComponent(companyMatch[1]) : '',
            run_id: runMatch ? runMatch[1] : ''
        };
    }

    document.addEventListener('DOMContentLoaded', function () {
        var root = document.getElementById('copilot-root');
        if (!root) return;

        var toggle = document.getElementById('copilot-toggle');
        var panel = document.getElementById('copilot-panel');
        var close = document.getElementById('copilot-close');
        var form = document.getElementById('copilot-form');
        var question = document.getElementById('copilot-question');
        var messages = document.getElementById('copilot-messages');
        var status = document.getElementById('copilot-status');
        var askUrl = root.getAttribute('data-ask-url');
        var csrfToken = root.getAttribute('data-csrf-token') || getCookie('csrftoken');

        function setOpen(open) {
            panel.classList.toggle('hidden', !open);
            toggle.setAttribute('aria-expanded', open ? 'true' : 'false');
            if (open) question.focus();
        }

        toggle.addEventListener('click', function () {
            setOpen(panel.classList.contains('hidden'));
        });
        close.addEventListener('click', function () {
            setOpen(false);
        });

        form.addEventListener('submit', function (event) {
            event.preventDefault();
            var value = question.value.trim();
            if (!value) {
                status.textContent = 'Enter a question first.';
                return;
            }

            appendMessage(messages, 'user', value);
            question.value = '';
            question.disabled = true;
            form.querySelector('button[type="submit"]').disabled = true;
            status.textContent = 'Checking portal data...';

            fetch(askUrl, {
                method: 'POST',
                credentials: 'same-origin',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRFToken': csrfToken
                },
                body: JSON.stringify({
                    question: value,
                    context: pageContext()
                })
            })
                .then(function (response) {
                    return response.json().then(function (data) {
                        data.status = response.status;
                        return data;
                    });
                })
                .then(function (data) {
                    if (data.success) {
                        appendMessage(messages, 'assistant', data.answer, data.sources, data.warnings);
                        status.textContent = data.request_id ? 'Request ' + data.request_id : '';
                    } else {
                        appendMessage(messages, 'assistant', (data.warnings || [data.error || 'Copilot request failed.']).join(' '), data.sources);
                        status.textContent = data.request_id ? 'Request ' + data.request_id : 'Request failed';
                    }
                })
                .catch(function () {
                    appendMessage(messages, 'assistant', 'Copilot request failed before the portal returned a response.');
                    status.textContent = 'Request failed';
                })
                .finally(function () {
                    question.disabled = false;
                    form.querySelector('button[type="submit"]').disabled = false;
                    question.focus();
                });
        });
    });
})();
