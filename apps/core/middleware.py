from urllib.parse import quote

from django.conf import settings
from django.shortcuts import redirect


class LoginRequiredMiddleware:
    """Require auth for all routes except selected public paths."""

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        full_path = request.get_full_path()
        public_prefixes = (
            settings.LOGIN_URL,
            "/logout/",
            "/admin/",  # Django admin handles its own auth
            "/static/",
        )
        path = request.path
        if not request.user.is_authenticated and not any(path.startswith(p) for p in public_prefixes):
            next_url = quote(full_path, safe="/")
            return redirect(f"{settings.LOGIN_URL}?next={next_url}")
        return self.get_response(request)
