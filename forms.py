from django import forms

class InsightlyzeForm(forms.Form):
    client_choices = (
        ('ATT', 'ATT'),
        ('Optus', 'Optus'),
        ('Kohls', 'Kohls'),
        ('Sirius XM Canada', 'Sirius XM Canada'),
        ('Best Buy Canada', 'Best Buy Canada'),
        ('BJs', 'BJs')
    )

    event_type_choices = (
        ('Highly negative customer sentiment', 'Highly negative customer sentiment'),
        ('Phone Referral', 'Phone Referral'),
        ('Chat Transfer', 'Chat Transfer'),
        ('Custom Search', 'Custom Search'),
        ('Order Confirmation', 'Order Confirmation'),
        ('Sale Attempt', 'Sale Attempt'),
        ('Highly positive customer sentiment', 'Highly positive customer sentiment')
    )

    period_choices = (
        ('1 week', 'last week'),
        ('2 week', '2nd last week'),
        ('3 week', '3rd last week'),
        ('4 week', '4th last week'),
        ('5 week', '1 month')
    )

    sender_choices = (
        ('agent', 'agent'),
        ('customer', 'customer')
    )

    client = forms.ChoiceField(choices=client_choices)
    event_type = forms.ChoiceField(choices=event_type_choices)
    period = forms.ChoiceField(choices=period_choices)
    sender = forms.ChoiceField(choices=sender_choices)
    custom_text = forms.CharField(widget=forms.Textarea)
