from django.core.exceptions import ValidationError
from django.test import TestCase, tag

from core.models import XSRConfiguration


@tag('unit')
class ModelTests(TestCase):

    def test_create_xsr_configuration(self):
        """Test that creating a new XSR Configuration entry is successful
        with defaults """
        xsr_api_endpoint = 'api/test_file'

        xiaConfig = XSRConfiguration(
            xsr_api_endpoint=xsr_api_endpoint)

        self.assertEqual(xiaConfig.xsr_api_endpoint,
                         xsr_api_endpoint)

    def test_create_two_xsr_configuration(self):
        """Test that trying to create more than one XSR Configuration throws
        ValidationError """
        with self.assertRaises(ValidationError):
            xsrConfig = \
                XSRConfiguration(xsr_api_endpoint="example1/api")
            xsrConfig2 = \
                XSRConfiguration(xsr_api_endpoint="example2/api")

            xsrConfig.save()
            xsrConfig2.save()
