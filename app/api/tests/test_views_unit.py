from unittest.mock import patch

from ddt import ddt
from django.test import tag
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase


@tag('unit')
@ddt
class ViewTests(APITestCase):

    def test_xia_workflow(self):
        """Test that the /api/xia-workflow/"""

        url = reverse('api:xia_workflow')
        with patch('api.views.'
                   'execute_xia_automated_workflow') as mock_workflow:
            class Test:
                id = 1

            mock_workflow.delay.return_value = Test
            response = self.client.get(url)

            self.assertEqual(mock_workflow.delay.call_count, 1)
            self.assertEqual(response.status_code,
                             status.HTTP_202_ACCEPTED)
