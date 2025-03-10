from django.db.models import Q
from rest_framework.pagination import PageNumberPagination
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
from rest_framework.parsers import MultiPartParser
import pandas as pd
import os
from django.conf import settings


class StandardResultsSetPagination(PageNumberPagination):
    page_size = 1
    page_size_query_param = 'page_size'
    max_page_size = 50


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
##################################################

class HuggingfaceDatasetsListView(APIView):
    def get(self, request):
        response = requests.get(f"https://huggingface.co/api/datasets?full=full&limit=30")
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
        res = requests.get(f"https://huggingface.co/api/datasets?full=full&limit=2")
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
            "license": data[0]['cardData']['license'][0],
            "tasks": data[0]['cardData']['task_categories'][0],
            "datasetDate": data[0]['createdAt'],
            "columnDataType": data[0]['cardData']['dataset_info']['features'],
            "sourceJson": data[0],
        }
        print(result)
        ser = DatasetSerializer(data=result, context={'request': request})
        if ser.is_valid():
            ser.validated_data['user'] = request.user
            instance = ser.save()
            return Response({"response": "Added"}, status=status.HTTP_201_CREATED)
        return Response(ser.errors, status=400)


class BulkImportHuggingfaceView(APIView):
    #permission_classes = [IsAuthenticated, BlocklistPermission]
    # permission_classes = [IsAdminUser]
    def post(self, request):
        res = requests.get(f"https://huggingface.co/api/datasets?full=full&limit=100")
        dataList = res.json()
        num_added_records = 0

        for data in dataList:
            dataset_info = data['cardData']['dataset_info']

            result = {
                #"code": data['symbol'],
                "name": data['id'],
                "owner": data['author'],
                "internalId": data['_id'],
                "internalCode": data['id'],
                #"recordsNum": data['symbol'],
                "size": dataset_info[0]['dataset_size'] if isinstance(dataset_info, list) else dataset_info['dataset_size'],
                #"format": data['symbol'],
                "language": data['cardData']['language'],
                "desc": data['description'],
                "license": data['cardData']['license'][0],
                "tasks": data['cardData']['task_categories'][0],
                "datasetDate": data['createdAt'],
                "columnDataType": dataset_info[0]['features'] if isinstance(dataset_info, list) else dataset_info['features'],
                "sourceJson": data,
            }

            ser = DatasetSerializer(data=result, context={'request': request})
            if ser.is_valid():
                ser.validated_data['user'] = request.user
                instance = ser.save()
                num_added_records = num_added_records + 1

        return Response({"response": f"{num_added_records} Records Added"}, status=status.HTTP_201_CREATED)

##################################################
# Kaggle
##################################################
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
##################################################

class UsersListView(APIView):
    serializer_class = UserSerializer
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
##################################################

class DatasetsListView(APIView):
    def get(self, request):
        queryset = Dataset.objects.all()
        paginator = LimitOffsetPagination()
        result = paginator.paginate_queryset(queryset=queryset, request=request)
        ser = DatasetSerializer(instance=result, many=True, context={"request": request})
        return Response(data=ser.data)


class DatasetDetailView(APIView):
    def get(self, request, pk):
        serializer_class = DatasetSerializer
        instance = Dataset.objects.get(id=pk)
        ser = DatasetSerializer(instance=instance)
        return Response(data=ser.data)


class AddDatasetView(APIView):
    serializer_class = DatasetSerializer
    parser_classes = [MultiPartParser]
    permission_classes = [IsAuthenticated, BlocklistPermission]
    # permission_classes = [IsAdminUser]
    def post(self, request):
        ser = DatasetSerializer(data=request.data, context={'request': request})
        if ser.is_valid():
            ser.validated_data['user'] = request.user
            instance = ser.save()
            return Response({"response": "Added"}, status=status.HTTP_201_CREATED)
        return Response(ser.errors, status=status.HTTP_400_BAD_REQUEST)


class UpdateDatasetView(APIView):
    serializer_class = DatasetSerializer
    parser_classes = [MultiPartParser]
    permission_classes = [IsAuthenticated, IsOwnerOrReadOnly]
    def put(self, request, pk):
        instance = Dataset.objects.get(id=pk)
        self.check_object_permissions(request, instance)
        ser = DatasetSerializer(instance=instance, data=request.data, partial=True)
        if ser.is_valid():
            instance = ser.save()
            return Response({"response": "Updated"}, status=status.HTTP_200_OK)
        return Response(ser.errors, status=status.HTTP_400_BAD_REQUEST)


class DeleteDatasetView(APIView):
    serializer_class = DatasetSerializer
    parser_classes = [MultiPartParser]
    permission_classes = [IsAuthenticated, IsOwnerOrReadOnly]
    def delete(self, request, pk):
        instance = Dataset.objects.get(id=pk)
        instance.delete()
        return Response({"response": "Deleted"}, status=status.HTTP_200_OK)


class SearchDatasetView(APIView, StandardResultsSetPagination):
    """ example : /dataset/search?q=aaa"""
    serializer_class = DatasetSerializer
    def get(self, request):
        q = request.GET.get('q')
        queryset = Dataset.objects.filter(Q(name__icontains=q) | Q(desc__icontains=q))
        result = self.paginate_queryset(queryset, request)
        ser = DatasetSerializer(instance=result, many=True)
        return Response(data=ser.data, status=status.HTTP_200_OK)


class DatasetSampleView(APIView):
    def get(self, request):
        dataset_id = request.GET.get('dataset_id')
        df = pd.read_parquet(os.path.join(settings.MEDIA_ROOT, 'datasets/1.parquet'))
        df_out = df[0:20]
        json_output = df_out.to_json(orient='records')
        return Response({"response": json_output})

##################################################
# Comments
##################################################

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



##################################################
# DownloadScheduler
##################################################
# pip install scheduler

import datetime as dt
from scheduler import Scheduler
from scheduler.trigger import Monday, Tuesday

class DownloadSchedulerView(APIView):

    def foo():
        print("foo")

    def get(self, request):
        #repo_id = request.GET.get('repo_id')
        schedule = Scheduler()
        # schedule.cyclic(dt.timedelta(seconds=5), self.foo)
        schedule.cyclic(dt.timedelta(seconds=5), print('oook'))
        return Response({"response": "schedule finish"})



##################################################
# DataConvertor
##################################################
# pip install Cython
# pip install fastparquet

import pandas as pd
from fastparquet import write, ParquetFile


class DataConvertorView(APIView):

    def get(self, request):
        dataset_id = request.GET.get('dataset_id')
        response = requests.get(f"https://huggingface.co/api/datasets/{dataset_id}/parquet")
        datasets = response.json()
        print(datasets['default']['test'])
        # df_dataset = pd.read_csv(f'datasets/{dataset_id}.csv')
        # write(f'{dataset_id}.parq', df_dataset, compression='GZIP')
        # return Response({"response": f"{dataset_id}.csv Converted to {dataset_id}.parq"})
        return Response({"response": datasets})


##################################################
# GenerateMetaData
##################################################
class GenerateMetaData(APIView):
    #permission_classes = [IsAuthenticated, BlocklistPermission]
    # permission_classes = [IsAdminUser]
    def post(self, request):
        data = request.data
        #data = res.json()
        result = {
            #"code": data[0]['symbol'],
            "name": data['name'],
            "owner": data['owner'],
            "internalId": data['internalId'],
            "internalCode": data['internalCode'],
            "recordsNum": data['recordsNum'],
            "size": data['size'],
            "format": data['format'],
            "language": data['language'],
            "desc": data['desc'],
            "license": data['license'],
            "tasks": data['tasks'],
            "datasetDate": data['datasetDate'],
            "columnDataType": data['columnDataType'],
            #"sourceJson": data[sourceJson],
        }
        print(result)


        ser = DatasetSerializer(data=result, context={'request': request})
        if ser.is_valid():
            ser.validated_data['user'] = request.user
            instance = ser.save()
            return Response({"response": "Added"}, status=status.HTTP_201_CREATED)
        return Response(ser.errors, status=400)

