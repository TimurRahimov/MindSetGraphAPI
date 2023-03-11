from django.urls import path, include
from .views import TestView, NebulaAPIView

urlpatterns = [
    path('', TestView),
    path('api/v1/nebula/', NebulaAPIView.as_view())
]
