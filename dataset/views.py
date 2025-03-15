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
from .serializers import UserSerializer, UserDetailSerializer, UserFullDetailSerializer, DatasetSerializer,InternationalDatasetSerializer, CommentSerializer
from .models import Dataset, Comment
from .permissions import BlocklistPermission, IsOwnerOrReadOnly
from rest_framework.viewsets import ViewSet, ModelViewSet
from rest_framework.pagination import PageNumberPagination, LimitOffsetPagination
from rest_framework.parsers import MultiPartParser
from apscheduler.schedulers.background import BackgroundScheduler
from django.conf import settings
import kaggle
import json
import os

class StandardResultsSetPagination(PageNumberPagination):
    page_size = 1
    page_size_query_param = 'page_size'
    max_page_size = 50


##################################################
# Huggingface
##################################################

class HuggingfaceDatasetsListView(APIView):
    def get(self, request):
        page = request.GET.get('page')
        limit = request.GET.get('limit')
        response = requests.get(f"https://huggingface.co/api/datasets?full=full&page={page}&limit={limit}")
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
            #"size": data[0]['cardData']['dataset_info']['dataset_size'],
            #"format": data[0]['symbol'],
            "language": data[0]['cardData']['language'][0],
            "desc": data[0]['description'],
            "license": data[0]['cardData']['license'][0],
            "tasks": data[0]['cardData']['task_categories'][0],
            "datasetDate": data[0]['createdAt'],
            #"columnDataType": data[0]['cardData']['dataset_info']['features'],
            "sourceJson": data[0],
        }
        print(result)
        ser = InternationalDatasetSerializer(data=result, context={'request': request})
        if ser.is_valid():
            ser.validated_data['user'] = request.user
            instance = ser.save()
            return Response({"response": "Added"}, status=status.HTTP_201_CREATED)
        return Response(ser.errors, status=400)


class BulkImportHuggingfaceView(APIView):
    permission_classes = [IsAuthenticated, BlocklistPermission]

    def post(self, request):
        page = 2
        limit = 5000
        res = requests.get(f"https://huggingface.co/api/datasets?full=full&limit=5000")
        dataList = res.json()
        num_added_records = 0

        for data in dataList:
            try:
                card_data = data.get('cardData', {})
                dataset_info = card_data.get('dataset_info', {})

                # If dataset_info is a list, take the first item
                if isinstance(dataset_info, list) and dataset_info:
                    dataset_info = dataset_info[0]

                # Extract language and ensure it's a string
                language = card_data.get('language')
                if isinstance(language, list):
                    language = ", ".join(language)  # Convert list to comma-separated string

                result = {
                    "name": data.get('id'),
                    "owner": data.get('author'),
                    "internalId": data.get('_id'),
                    "internalCode": data.get('id'),
                    "language": language,  # Ensures it's a string or None
                    "desc": data.get('description'),
                    "license": card_data.get('license', [None])[0],  # Avoid IndexError
                    "tasks": card_data.get('task_categories', [None])[0],
                    "datasetDate": data.get('createdAt'),
                    "size": dataset_info.get('dataset_size'),
                    "format":  next((tag.split(":")[1] for tag in data.get('tags') if tag.startswith("format:")), None),
                    "dataType": next((tag.split(":")[1] for tag in data.get('tags') if tag.startswith("modality:")), None),
                    "columnDataType": dataset_info.get('features'),
                    "dataset_tags": ', '.join(card_data.get('tags')) + ', ' + ', '.join(card_data.get('task_categories')),
                    "likes": data.get('likes'),
                    "downloads": data.get('downloads'),
                    "sourceJson": data,
                }

                # Serialize and validate data
                ser = InternationalDatasetSerializer(data=result, context={'request': request})
                if ser.is_valid():
                    ser.validated_data['user'] = request.user  # Set user before saving
                    ser.save()
                    num_added_records += 1
                else:
                    print(f"Validation error: {ser.errors}")  # Debugging step

            except Exception as e:
                print(f"Error processing dataset {data.get('id', 'Unknown')}: {e}")

        return Response({"response": f"{num_added_records} Records Added"}, status=status.HTTP_201_CREATED)
##################################################
# Kaggle
##################################################

class KaggleDatasetDetailView1(APIView):
    def get(self, request):
        dataset_ref = request.GET.get('dataset_ref')
        try:
            # Load Kaggle credentials from JSON
            kaggle_config_path = 'config/kaggle.json'
            if not os.path.exists(kaggle_config_path):
                return Response({"error": "Kaggle credentials file not found"}, status=status.HTTP_400_BAD_REQUEST)

            with open(kaggle_config_path) as f:
                kaggle_auth = json.load(f)

            # Set Kaggle API credentials
            os.environ['KAGGLE_USERNAME'] = kaggle_auth.get('username', '')
            os.environ['KAGGLE_KEY'] = kaggle_auth.get('key', '')

            # Authenticate and get datasets
            kaggle.api.authenticate()
            dataset = kaggle.api.dataset_metadata(dataset_ref)

            return Response({"dataset": dataset}, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class KaggleDatasetDetailView(APIView):
    def get(self, request):
        dataset_ref = request.GET.get('dataset_ref')

        if not dataset_ref:
            return Response({"error": "dataset_ref parameter is required"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            kaggle_config_path = 'config/kaggle.json'
            if not os.path.exists(kaggle_config_path):
                return Response({"error": "Kaggle credentials file not found"}, status=status.HTTP_400_BAD_REQUEST)

            with open(kaggle_config_path) as f:
                kaggle_auth = json.load(f)

            # Set Kaggle API credentials
            os.environ['KAGGLE_USERNAME'] = kaggle_auth.get('username', '')
            os.environ['KAGGLE_KEY'] = kaggle_auth.get('key', '')

            # Initialize and authenticate Kaggle API
            api = kaggle.KaggleApi()
            api.authenticate()
            dataset = api.dataset_metadata(dataset_ref)

            # Convert dataset object to dictionary
            dataset_info = {
                "title": dataset.title,
                "owner": dataset.creatorName,
                "ref": dataset.ref,
                "size": dataset.size,
                "license": dataset.licenseName,
                "totalViews": dataset.totalViews,
                "totalDownloads": dataset.totalDownloads,
                "usabilityRating": dataset.usabilityRating,
                "tags": dataset.tags,
                "fileTypes": dataset.fileTypes,
                "url": f"https://www.kaggle.com/datasets/{dataset_ref}"
            }
            return Response({"dataset": dataset_info}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"error": f"Unexpected error: {str(e)}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class KaggleDatasetsListView1(APIView):
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


class KaggleDatasetsListView2(APIView):
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


class KaggleDatasetsListView(APIView):
    def get(self, request):
        try:
            # Load Kaggle credentials from JSON
            kaggle_config_path = 'config/kaggle.json'
            if not os.path.exists(kaggle_config_path):
                return Response({"error": "Kaggle credentials file not found"}, status=status.HTTP_400_BAD_REQUEST)

            with open(kaggle_config_path) as f:
                kaggle_auth = json.load(f)

            # Set Kaggle API credentials
            os.environ['KAGGLE_USERNAME'] = kaggle_auth.get('username', '')
            os.environ['KAGGLE_KEY'] = kaggle_auth.get('key', '')

            # Authenticate and get datasets
            kaggle.api.authenticate()
            datasets = kaggle.api.dataset_list()

            # Convert dataset objects to a dictionary format
            data = [{"title": d.title, "size": d.size, "ref": d.ref, "url": d.url, "lastUpdated": d.lastUpdated} for d in datasets]

            return Response({"datasets": datasets}, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class KaggleDatasetsListView3(APIView):
    def get(self, request):
        try:
            # Load Kaggle credentials from JSON
            kaggle_config_path = 'config/kaggle.json'
            if not os.path.exists(kaggle_config_path):
                return Response({"error": "Kaggle credentials file not found"},
                                status=status.HTTP_400_BAD_REQUEST)

            with open(kaggle_config_path) as f:
                kaggle_auth = json.load(f)

            # Set Kaggle API credentials
            os.environ['KAGGLE_USERNAME'] = kaggle_auth.get('username', '')
            os.environ['KAGGLE_KEY'] = kaggle_auth.get('key', '')

            # Authenticate and get datasets
            kaggle.api.authenticate()

            response = requests.get(f"https://www.kaggle.com/api/v1/datasets/list?group=public&sortby=hottest&size=all&filetype=all&license=all&viewed=unspecified&page=1")



            return Response({"response": response}, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


##################################################
# PaperWithCode
##################################################

class PaperWithCodeDatasetsListView(APIView):
    def get(self, request):
        response = requests.get(f"https://paperswithcode.com/api/v1/datasets/?page=150&items_per_page=50")
        data = response.json()
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



##################################################
# DownloadScheduler
##################################################
#  pip install apscheduler
class DownloadSchedulerView(APIView):
    def foo(self):
        print("foo")

    def get(self, request):
        scheduler = BackgroundScheduler()
        scheduler.add_job(self.foo, 'interval', seconds=5)  # Run every 5 seconds
        scheduler.start()

        return Response({"response": "schedule started"})

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

