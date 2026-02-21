from __future__ import annotations

from datetime import date
from zoneinfo import ZoneInfo

from django.conf import settings
from django import forms

from .models import RunJob, RunSchedule, validate_cron_expr, validate_timezone_name


def _int_setting(name: str, default: int, *, minimum: int = 0) -> int:
    raw = getattr(settings, name, default)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    if value < minimum:
        return default
    return value


def _default_parallel() -> int:
    return _int_setting("OIAT_DASHBOARD_DEFAULT_PARALLEL", 2, minimum=1)


def _default_stagger_seconds() -> int:
    return _int_setting("OIAT_DASHBOARD_DEFAULT_STAGGER_SECONDS", 2, minimum=0)


def _default_schedule_timezone() -> str:
    return str(
        getattr(
            settings,
            "OIAT_BUSINESS_TIMEZONE",
            getattr(settings, "TIME_ZONE", "UTC"),
        )
    )


class RunTriggerForm(forms.Form):
    scope = forms.ChoiceField(choices=[("single_company", "Single Company"), ("all_companies", "All Companies")])
    company_key = forms.CharField(required=False)
    date_mode = forms.ChoiceField(choices=[("yesterday", "Yesterday"), ("target_date", "Target Date"), ("range", "Date Range")])
    target_date = forms.DateField(required=False)
    from_date = forms.DateField(required=False)
    to_date = forms.DateField(required=False)
    skip_download = forms.BooleanField(required=False)
    parallel = forms.IntegerField(required=False, min_value=1)
    stagger_seconds = forms.IntegerField(required=False, min_value=0)
    continue_on_failure = forms.BooleanField(required=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["parallel"].initial = _default_parallel()
        self.fields["stagger_seconds"].initial = _default_stagger_seconds()

    def clean(self):
        cleaned = super().clean()
        scope = cleaned.get("scope")
        company_key = (cleaned.get("company_key") or "").strip()
        date_mode = cleaned.get("date_mode")
        target_date = cleaned.get("target_date")
        from_date = cleaned.get("from_date")
        to_date = cleaned.get("to_date")
        skip_download = cleaned.get("skip_download")
        parallel = cleaned.get("parallel")
        stagger_seconds = cleaned.get("stagger_seconds")
        continue_on_failure = cleaned.get("continue_on_failure")

        if scope == "single_company" and not company_key:
            self.add_error("company_key", "Company key is required for single-company runs.")
        if scope == "all_companies":
            cleaned["company_key"] = ""
            cleaned["parallel"] = int(parallel or _default_parallel())
            cleaned["stagger_seconds"] = int(stagger_seconds or _default_stagger_seconds())
            cleaned["continue_on_failure"] = bool(continue_on_failure)
        else:
            cleaned["parallel"] = 1
            cleaned["stagger_seconds"] = _default_stagger_seconds()
            cleaned["continue_on_failure"] = False

        if date_mode == "target_date" and not target_date:
            self.add_error("target_date", "Target date is required.")
        if date_mode == "range":
            if not from_date or not to_date:
                self.add_error("from_date", "from_date and to_date are required in range mode.")
            elif from_date > to_date:
                self.add_error("to_date", "to_date must be >= from_date.")
        if skip_download and date_mode != "range":
            self.add_error("skip_download", "skip_download is only valid for range mode.")

        if date_mode != "target_date":
            cleaned["target_date"] = None
        if date_mode != "range":
            cleaned["from_date"] = None
            cleaned["to_date"] = None
            cleaned["skip_download"] = False

        return cleaned


class CompanyBasicForm(forms.Form):
    company_key = forms.SlugField(max_length=64)
    display_name = forms.CharField(max_length=255)
    realm_id = forms.CharField(max_length=64)
    deposit_account = forms.CharField(max_length=255)
    tax_mode = forms.CharField(max_length=64, initial="vat_inclusive_7_5")
    epos_username_env_key = forms.CharField(max_length=128)
    epos_password_env_key = forms.CharField(max_length=128)
    csv_prefix = forms.CharField(max_length=128, initial="sales_receipts")
    metadata_file = forms.CharField(max_length=128, initial="last_transform.json")
    uploaded_docnumbers_file = forms.CharField(max_length=128, initial="uploaded_docnumbers.json")
    slack_webhook_env_key = forms.CharField(max_length=128, required=False)


class CompanyAdvancedForm(forms.Form):
    trading_day_enabled = forms.BooleanField(required=False)
    trading_day_start_hour = forms.IntegerField(min_value=0, max_value=23, initial=5)
    trading_day_start_minute = forms.IntegerField(min_value=0, max_value=59, initial=0)

    inventory_enabled = forms.BooleanField(required=False)
    allow_negative_inventory = forms.BooleanField(required=False)
    inventory_start_date = forms.CharField(max_length=32, required=False, initial="today")
    default_qty_on_hand = forms.IntegerField(required=False, initial=0)

    tax_rate = forms.FloatField(required=False)
    tax_code_id = forms.CharField(max_length=64, required=False)
    tax_code_name = forms.CharField(max_length=128, required=False)

    date_format = forms.CharField(max_length=32, initial="%Y-%m-%d")
    receipt_prefix = forms.CharField(max_length=16, initial="SR")
    receipt_number_format = forms.CharField(max_length=64, initial="date_tender_sequence")
    group_by = forms.CharField(max_length=128, initial="date,tender")

    def cleaned_group_by(self) -> list[str]:
        raw = (self.cleaned_data.get("group_by") or "").strip()
        return [p.strip() for p in raw.split(",") if p.strip()]

    @staticmethod
    def today_iso() -> str:
        return date.today().isoformat()


class RunScheduleForm(forms.ModelForm):
    class Meta:
        model = RunSchedule
        fields = [
            "name",
            "enabled",
            "scope",
            "company_key",
            "cron_expr",
            "timezone_name",
            "target_date_mode",
            "parallel",
            "stagger_seconds",
            "continue_on_failure",
        ]
        widgets = {
            "target_date_mode": forms.HiddenInput(),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["scope"].choices = RunJob.SCOPE_CHOICES
        self.fields["target_date_mode"].initial = RunSchedule.TARGET_DATE_MODE_TRADING_DATE
        self.fields["timezone_name"].initial = self.initial.get("timezone_name") or _default_schedule_timezone()
        self.fields["parallel"].min_value = 1
        self.fields["stagger_seconds"].min_value = 0
        self.fields["parallel"].initial = self.initial.get("parallel", _default_parallel())
        self.fields["stagger_seconds"].initial = self.initial.get("stagger_seconds", _default_stagger_seconds())

    def clean_cron_expr(self) -> str:
        value = (self.cleaned_data.get("cron_expr") or "").strip()
        try:
            validate_cron_expr(value)
        except Exception as exc:
            raise forms.ValidationError("Enter a valid cron expression.") from exc
        return value

    def clean_timezone_name(self) -> str:
        value = (self.cleaned_data.get("timezone_name") or "").strip()
        if not value:
            value = _default_schedule_timezone()
        try:
            validate_timezone_name(value)
        except Exception as exc:
            raise forms.ValidationError("Enter a valid timezone name (e.g. Africa/Lagos).") from exc
        return value

    def clean_target_date_mode(self) -> str:
        value = (self.cleaned_data.get("target_date_mode") or "").strip()
        if value != RunSchedule.TARGET_DATE_MODE_TRADING_DATE:
            return RunSchedule.TARGET_DATE_MODE_TRADING_DATE
        return value

    def clean(self):
        cleaned = super().clean()
        scope = cleaned.get("scope")
        company_key = (cleaned.get("company_key") or "").strip()

        if scope == RunJob.SCOPE_SINGLE and not company_key:
            self.add_error("company_key", "Company key is required for single-company schedules.")

        if scope == RunJob.SCOPE_ALL:
            cleaned["company_key"] = None
        else:
            cleaned["company_key"] = company_key
            cleaned["parallel"] = 1
            cleaned["continue_on_failure"] = False

        timezone_name = cleaned.get("timezone_name")
        if timezone_name:
            try:
                ZoneInfo(timezone_name)
            except Exception:
                self.add_error("timezone_name", "Invalid timezone name.")

        cleaned["target_date_mode"] = RunSchedule.TARGET_DATE_MODE_TRADING_DATE
        return cleaned
