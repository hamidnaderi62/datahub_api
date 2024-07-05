from django.shortcuts import render
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework.views import APIView
import requests
from rest_framework import status
from rest_framework.authentication import TokenAuthentication
from rest_framework.permissions import IsAuthenticated, IsAdminUser
from django.contrib.auth.models import User
from .serializers import UserSerializer, UserDetailSerializer, UserFullDetailSerializer, DatasetSerializer, CommentSerializer
from .models import Dataset, Comment
from .permissions import BlocklistPermission, IsOwnerOrReadOnly
from rest_framework.viewsets import ViewSet, ModelViewSet
from rest_framework.pagination import PageNumberPagination, LimitOffsetPagination
import pandas as pd
import os
from django.conf import settings

@api_view(['GET', 'POST'])
def hello_world(request):
    data = request.data
    return Response({"message": f"{data['fname']}  {data.get('lname')}"})


class HelloWorld(APIView):
    def get(self, request):
        return Response({"message": "Hi get"})
    def post(self, request):
        return Response({"message": "Hi post"})


class CryptoPriceListView(APIView):
    def get(self, request):
        coin = request.GET.get('coin')
        response = requests.get(f"https://api.binance.com/api/v3/ticker/price?symbol={coin.upper()}")
        data = response.json()
        result = {
            "symbol": data['symbol'],
            "price": data['price']
        }
        return Response(data=result)

##################################################
# Huggingface

class HuggingfaceDatasetsListView(APIView):
    def get(self, request):
        response = requests.get(f"https://huggingface.co/api/datasets?full=full&limit=10")
        data = response.json()
        result = {
            "data": data
        }
        return Response(data=result)


class HuggingfaceDatasetDetailView(APIView):
    def get(self, request):
        repo_id = request.GET.get('repo_id')
        response = requests.get(f"https://huggingface.co/api/datasets/{repo_id}/croissant")
        data = response.json()
        result = {
            "data": data
        }
        return Response(data=result)


class ImportHuggingfaceView(APIView):
    #permission_classes = [IsAuthenticated, BlocklistPermission]
    # permission_classes = [IsAdminUser]
    def post(self, request):
        res = requests.get(f"https://huggingface.co/api/datasets?full=full&limit=1")
        data = res.json()
        result = {
            #"code": data[0]['symbol'],
            "name": data[0]['id'],
            "owner": data[0]['author'],
            "internalId": data[0]['_id'],
            "internalCode": data[0]['id'],
            #"recordsNum": data[0]['symbol'],
            "size": data[0]['cardData']['dataset_info']['dataset_size'],
            #"format": data[0]['symbol'],
            "language": data[0]['cardData']['language'][0],
            "desc": data[0]['description'],
            "datasetDate": data[0]['createdAt'],
            "columnDataType": data[0]['cardData']['dataset_info']['features'],
        }
        print(result)
        ser = DatasetSerializer(data=result, context={'request': request})
        if ser.is_valid():
            ser.validated_data['user'] = request.user
            instance = ser.save()
            return Response({"response": "Added"}, status=status.HTTP_201_CREATED)
        return Response(ser.errors, status=400)


##################################################
# Kaggle
class KaggleDatasetsListView(APIView):
    def get(self, request):
        import kaggle
        from kaggle.api.kaggle_api_extended import KaggleApi
        api = KaggleApi()
        api.authenticate()
        response = api.model_list_cli()
        # response = requests.get(f"https://huggingface.co/api/datasets")
        data = response
        result = {
            "data": data
        }
        return Response(data=result)


class KaggleDatasetsListView1(APIView):
    def get(self, request):
        import json
        import os
        import kaggle

        with open('config/kaggle.json') as f:
            kaggle_auth = json.load(f)
            print(kaggle_auth)

        os.environ['KAGGLE_USERNAME'] = kaggle_auth['username']  # manually input My_Kaggle User_Name
        os.environ['KAGGLE_KEY'] = kaggle_auth['key']  # manually input My_Kaggle Key

        kaggle.api.authenticate()
        response = kaggle.api.dataset_list_cli()
        # response = requests.get(f"https://huggingface.co/api/datasets")
        data = response
        result = {
            "data": data
        }
        return Response(data=result)

##################################################
# Users
class UsersListView(APIView):
    def get(self, request):
        queryset = User.objects.all()
        ser = UserSerializer(instance=queryset, many=True)
        return Response(data=ser.data)


class UsersListDetailView(APIView):
    def get(self, request):
        users = User.objects.all()
        serializer = UserDetailSerializer(instance=users, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)


class UsersListFullDetailView(APIView):
    def get(self, request):
        users = User.objects.all()
        serializer = UserFullDetailSerializer(instance=users, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)


class AddUserView(APIView):
    def post(self, request):
        ser = UserSerializer(data=request.data, context={'request': request})
        if ser.is_valid():
            instance = ser.save()
            return Response({"response": "Added"}, status=status.HTTP_201_CREATED)
        return Response(ser.errors, status=400)


class CheckToken(APIView):
    # authentication_classes = [TokenAuthentication]
    def get(self, request):
        user = request.user
        return Response({"user": user.username}, status=status.HTTP_200_OK)
##################################################
# Datasets


class DatasetsListView(APIView):
    def get(self, request):
        queryset = Dataset.objects.all()
        paginator = LimitOffsetPagination()
        result = paginator.paginate_queryset(queryset=queryset, request=request)
        ser = DatasetSerializer(instance=result, many=True, context={"request": request})
        return Response(data=ser.data)


class DatasetDetailView(APIView):
    def get(self, request, pk):
        instance = Dataset.objects.get(id=pk)
        ser = DatasetSerializer(instance=instance)
        return Response(data=ser.data)


class AddDatasetView(APIView):
    permission_classes = [IsAuthenticated, BlocklistPermission]
    # permission_classes = [IsAdminUser]
    def post(self, request):
        ser = DatasetSerializer(data=request.data, context={'request': request})
        if ser.is_valid():
            ser.validated_data['user'] = request.user
            instance = ser.save()
            return Response({"response": "Added"}, status=status.HTTP_201_CREATED)
        return Response(ser.errors, status=400)


class UpdateDatasetView(APIView):
    permission_classes = [IsAuthenticated, IsOwnerOrReadOnly]
    def put(self, request, pk):
        instance = Dataset.objects.get(id=pk)
        self.check_object_permissions(request, instance)
        ser = DatasetSerializer(instance=instance, data=request.data, partial=True)
        if ser.is_valid():
            instance = ser.save()
            return Response({"response": "Updated"})
        return Response(ser.errors)


class DeleteDatasetView(APIView):
    def delete(self, request, pk):
        instance = Dataset.objects.get(id=pk)
        instance.delete()
        return Response({"response": "Deleted"})


class DatasetSampleView(APIView):
    def get(self, request):
        dataset_id = request.GET.get('dataset_id')
        df = pd.read_parquet(os.path.join(settings.MEDIA_ROOT, 'datasets/2.parquet'))
        df_out = df[0:20]
        json_output = df_out.to_json(orient='records')
        return Response({"response": json_output})

##################################################
# Comments


class CommentsListView(APIView):
    def get(self, request, pk):
        comments = Dataset.objects.get(id=pk).comments.all()
        serializer = CommentSerializer(instance=comments, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)


# ViewSet
'''
class PretermViewSet(ViewSet):
    permission_classes = [IsAuthenticated]
    def list(self, request):
        queryset = Preterm.objects.all()
        serializer = PretermSerializer(instance=queryset, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    def retrieve(self, request, pk=None):
        instance = Preterm.objects.get(id=pk)
        serializer = PretermSerializer(instance=instance)
        return Response(serializer.data, status=status.HTTP_200_OK)

    def create(self, request):
        ser = PretermSerializer(data=request.data, context={'request': request})
        if ser.is_valid():
            ser.validated_data['user'] = request.user
            instance = ser.save()
            return Response({"response": "Added"}, status=status.HTTP_201_CREATED)
        return Response(ser.errors, status=400)

    def update(self, request, pk=None):
        instance = Preterm.objects.get(id=pk)
        self.check_object_permissions(request, instance)
        ser = PretermSerializer(instance=instance, data=request.data, partial=True)
        if ser.is_valid():
            instance = ser.save()
            return Response({"response": "Updated"})
        return Response(ser.errors)
'''

class DatasetViewSet(ModelViewSet):
    queryset = Dataset.objects.all()
    serializer_class = DatasetSerializer