import logging
from unittest.mock import patch

from django.test import tag

from core.tasks import execute_xia_automated_workflow

from .test_setup import TestSetUp

logger = logging.getLogger('dict_config_logger')


@tag('unit')
class TasksTests(TestSetUp):

    @patch("core.tasks.execute_xia_automated_workflow.run")
    def test_execute_xia_automated_workflow(self, mock_run):
        """Testing the working of xia workflow celery task queue"""
        self.assert_(execute_xia_automated_workflow.run())

        self.assert_(execute_xia_automated_workflow.run())
        self.assertEqual(mock_run.call_count, 2)

        self.assert_(execute_xia_automated_workflow.run())
        self.assertEqual(mock_run.call_count, 3)

    def test_check_calls_xia_workflow(self):
        """Testing the calls to commands from task list"""

        with patch('core.tasks.extract_Command.handle') as mock_extract, \
                patch('core.tasks.validate_source_Command.'
                      'handle') as mock_validate_source, \
                patch('core.tasks.transform_Command.'
                      'handle') as mock_transform, \
                patch('core.tasks.'
                      'validate_target_Command.'
                      'handle') as mock_validate_target, \
                patch('core.tasks.load_Command.handle') as mock_load, \
                patch('core.tasks.'
                      'load_supplemental_Command.'
                      'handle') as mock_load_supplemental:
            execute_xia_automated_workflow.run()

            self.assertEqual(mock_extract.call_count, 1)
            self.assertEqual(mock_validate_source.call_count, 1)
            self.assertEqual(mock_transform.call_count, 1)
            self.assertEqual(mock_validate_target.call_count, 1)
            self.assertEqual(mock_load.call_count, 1)
            self.assertEqual(mock_load_supplemental.call_count, 1)
