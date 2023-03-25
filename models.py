from django.db import models

class UserInput(models.Model):
    client = models.CharField(max_length=50)
    event_type = models.CharField(max_length=100)
    period = models.CharField(max_length=50)
    sender = models.CharField(max_length=50)
    custom_text = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)
