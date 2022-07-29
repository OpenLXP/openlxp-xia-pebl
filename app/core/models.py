from django.db import models
from django.forms import ValidationError


class XSRConfiguration(models.Model):
    """Model for XSR Configuration """

    xsr_api_endpoint = models.CharField(
        help_text='Enter the XSR API endpoint',
        max_length=200
    )

    def save(self, *args, **kwargs):
        if not self.pk and XSRConfiguration.objects.exists():
            raise ValidationError('There can be only one XSRConfiguration '
                                  'instance')
        return super(XSRConfiguration, self).save(*args, **kwargs)
