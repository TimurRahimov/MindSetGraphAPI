from django.shortcuts import render, HttpResponse
from rest_framework import generics
from rest_framework.response import Response
from rest_framework.request import Request
from rest_framework.views import APIView

from .nebula_driver import NebulaDriver


# Create your views here.
def TestView(request):
    return HttpResponse("Nebula API")


class NebulaAPIView(APIView):

    def get(self, request: Request):
        vertexes = NebulaDriver.get_all_vertices()
        edges = NebulaDriver.get_all_edges()

        return Response({
            'vertexes': vertexes,
            'edges': edges
        })

    def post(self, request: Request):
        if 'fullname' in request.data:
            view = 'json'

            if 'view' in request.data:
                view = request.data['view']

            response = NebulaDriver.get_subgraph(request.data['fullname'], view)

            if view == 'json' or (
                    isinstance(response, dict) and "NebulaError" in response
            ):
                return Response(response)
            else:
                return HttpResponse(response)
        else:
            return Response({})
