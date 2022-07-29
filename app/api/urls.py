from django.urls import path
from rest_framework.routers import DefaultRouter

from api.views import WorkflowView

router = DefaultRouter()

app_name = 'api'

urlpatterns = [
    path('xia-workflow/', WorkflowView.as_view(), name='xia_workflow'),
]
