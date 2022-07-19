import logging

from celery.result import AsyncResult
from django.http import JsonResponse
from rest_framework import permissions, status
from rest_framework.decorators import permission_classes
from rest_framework.response import Response
from rest_framework.views import APIView

from core.tasks import execute_xia_automated_workflow

logger = logging.getLogger('dict_config_logger')


@permission_classes((permissions.AllowAny,))
class WorkflowView(APIView):
    """Handles HTTP requests for Metadata for XIS"""

    def get(self, request):
        logger.info('XIA workflow api')
        task = execute_xia_automated_workflow.delay()
        response_val = {"task_id": task.id}

        return Response(response_val, status=status.HTTP_202_ACCEPTED)


def get_status(request, task_id):
    task_result = AsyncResult(task_id)
    result = {
        "task_id": task_id,
        "task_status": task_result.status,
        "task_result": task_result.result
    }
    return JsonResponse(result, status=200)
