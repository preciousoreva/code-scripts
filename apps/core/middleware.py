from django.conf import settings
from django.shortcuts import redirect


class LoginRequiredMiddleware:
    """Require auth for all routes except selected public paths."""

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        path = request.path
        public_prefixes = (
            settings.LOGIN_URL,
            "/logout/",
            "/admin/",  # Django admin handles its own auth
            "/static/",
        )
        if not request.user.is_authenticated and not any(path.startswith(p) for p in public_prefixes):
            return redirect(f"{settings.LOGIN_URL}?next={path}")
        return self.get_response(request)
