from __future__ import annotations

from django import forms
from django.contrib.auth import get_user_model

User = get_user_model()


class AccountProfileForm(forms.ModelForm):
    """Form for users to edit their display name and email. Username is not editable here."""

    class Meta:
        model = User
        fields = ["first_name", "last_name", "email"]
        input_class = (
            "w-full border border-slate-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-slate-200 focus:border-slate-400 "
            "dark:bg-slate-700 dark:border-slate-600 dark:text-slate-100 dark:placeholder-slate-400 dark:focus:ring-slate-500 dark:focus:border-slate-500"
        )
        widgets = {
            "first_name": forms.TextInput(
                attrs={
                    "class": input_class,
                    "placeholder": "First name",
                    "autocomplete": "given-name",
                }
            ),
            "last_name": forms.TextInput(
                attrs={
                    "class": input_class,
                    "placeholder": "Last name",
                    "autocomplete": "family-name",
                }
            ),
            "email": forms.EmailInput(
                attrs={
                    "class": input_class,
                    "placeholder": "email@example.com",
                    "autocomplete": "email",
                }
            ),
        }
