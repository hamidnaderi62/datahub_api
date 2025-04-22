from django.db.models import Q
from rest_framework.pagination import PageNumberPagination
from django.shortcuts import render
from rest_framework.decorators import api_view
from rest_framework.response import Response
from django.http import JsonResponse
from rest_framework.views import APIView
import requests
from rest_framework import status
from rest_framework.authentication import TokenAuthentication
from rest_framework.permissions import IsAuthenticated, IsAdminUser
from django.contrib.auth.models import User
from .serializers import UserSerializer, UserDetailSerializer, UserFullDetailSerializer, DatasetSerializer,InternationalDatasetSerializer, CommentSerializer
from .models import Dataset, InternationalDataset, Comment
from .permissions import BlocklistPermission, IsOwnerOrReadOnly
from rest_framework.viewsets import ViewSet, ModelViewSet
from rest_framework.pagination import PageNumberPagination, LimitOffsetPagination
from rest_framework.parsers import MultiPartParser
from apscheduler.schedulers.background import BackgroundScheduler
from django.conf import settings
import kaggle
import json
import os
from django.core.files.base import ContentFile
from django.core.files.storage import default_storage
import hashlib
import asyncio
import aiohttp
from asgiref.sync import sync_to_async
import tempfile
from django.db import transaction
import zipfile
import io

class StandardResultsSetPagination(PageNumberPagination):
    page_size = 1
    page_size_query_param = 'page_size'
    max_page_size = 50


##################################################
# General
##################################################



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


# Async class for download files
class HuggingfaceParquetFilesView(APIView):

    async def fetch_and_save(self, session, url, file_path):
        """Asynchronously download and save a file."""
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    return {"error": f"Failed to download {url}"}
                # Save file asynchronously
                content = await response.read()
                default_storage.save(file_path, ContentFile(content))
                return {"success": file_path}
        except Exception as e:
            return {"error": f"Exception while downloading {url}: {str(e)}"}

    async def download_files(self, json_data, repo_id):
        """Handles multiple concurrent downloads."""
        tasks = []
        downloaded_files = {}
        async with aiohttp.ClientSession() as session:
            for dataset, categories in json_data.items():
                downloaded_files[dataset] = {}
                for category, urls in categories.items():
                    downloaded_files[dataset][category] = []
                    for url in urls:
                        # Generate a hashed filename
                        file_name = os.path.basename(url)
                        hashed_name = hashlib.sha256(f"{repo_id}".encode()).hexdigest()[:16]  # Use first 16 chars
                        file_path = f"downloads/{hashed_name}/{dataset}_{category}_{file_name}"
                        # Ensure directories exist
                        os.makedirs(os.path.dirname(default_storage.path(file_path)), exist_ok=True)
                        # Schedule async download
                        task = self.fetch_and_save(session, url, file_path)
                        tasks.append(task)
                        downloaded_files[dataset][category].append(file_path)
            # Run tasks concurrently
            results = await asyncio.gather(*tasks)
            # Check for errors in results
            for result in results:
                if "error" in result:
                    return JsonResponse(result, status=500)
        return downloaded_files



    async def update_database(self, repo_id, dataset_status, download_links):
        try:
            dataset = await sync_to_async(InternationalDataset.objects.get)(name=repo_id)
            dataset.dataset_status = dataset_status
            dataset.downloadLink = download_links
            await sync_to_async(dataset.save)()
        except InternationalDataset.DoesNotExist:
            return {"error": f"Repo with repo_id {repo_id} not found in the database."}

    async def get(self, request):
        """Handles GET requests asynchronously."""
        repo_id = request.GET.get("repo_id")
        if not repo_id:
            return JsonResponse({"error": "Missing repo_id parameter"}, status=400)
        try:
            # Fetch JSON data from Hugging Face API
            async with aiohttp.ClientSession() as session:
                async with session.get(f"https://huggingface.co/api/datasets/{repo_id}/parquet") as response:
                    if response.status != 200:
                        return JsonResponse(
                            {"error": f"Failed to fetch data from Hugging Face. Status: {response.status}"},
                            status=response.status)
                    json_data = await response.json()
            if not isinstance(json_data, dict):
                return JsonResponse({"error": "Invalid JSON format received from Hugging Face"}, status=400)
            # Download files asynchronously
            downloaded_files = await self.download_files(json_data, repo_id)
            update_res = await self.update_database(repo_id, 'Download_Completed', downloaded_files)

            return JsonResponse({"downloaded_files": downloaded_files})
        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)

    # CHANGE START: Removed asyncio.run(), now just await the get method directly
    async def dispatch(self, request):
        """Handle async dispatch without asyncio.run()."""
        if asyncio.iscoroutinefunction(self.get):  # Check if get is async
            return await self.get(request)  # Await the async get method directly
        return await super().dispatch(request)  # Call parent dispatch for regular flow
        # CHANGE END


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
        limit = 10
        res = requests.get(f"https://huggingface.co/api/datasets?full=full&limit={limit}")
        dataList = res.json()
        num_added_records = 0

        for data in dataList:
            try:
                card_data = data.get('cardData', {})
                dataset_info = card_data.get('dataset_info', {})

                dataset_format = ''
                if data.get('tags'):
                    dataset_format = next((tag.split(":")[1] for tag in data.get('tags') if tag.startswith("format:")), None)

                data_type = ''
                if data.get('tags'):
                    data_type = next((tag.split(":")[1] for tag in data.get('tags') if tag.startswith("modality:")), None)

                dataset_tags = ''
                if card_data.get('tags'):
                    dataset_tags = ', '.join(card_data.get('tags')) + ', ' + ', '.join(card_data.get('task_categories'))

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
                    "internalCode": data.get('_id'),
                    "language": language,  # Ensures it's a string or None
                    "desc": data.get('description'),
                    "license": card_data.get('license', [None])[0],  # Avoid IndexError
                    "tasks": card_data.get('task_categories', [None])[0],
                    "datasetDate": data.get('createdAt'),
                    "size": dataset_info.get('dataset_size'),
                    "format":  dataset_format,
                    "dataType": data_type,
                    "columnDataType": dataset_info.get('features'),
                    "dataset_tags": dataset_tags,
                    "likes": data.get('likes'),
                    "downloads": data.get('downloads'),
                    "referenceOwner": 'HuggingFace',
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

# get kaggle dataset info
class KaggleDatasetDetailView(APIView):
    def get(self, request):
        kaggle_temp_path = "./temp/kaggle/"
        dataset_ref = request.GET.get('dataset_ref')

        if not dataset_ref:
            return Response({"error": "dataset_ref parameter is required"},
                            status=status.HTTP_400_BAD_REQUEST)

        try:
            # Check and load Kaggle credentials
            kaggle_config_path = 'config/kaggle.json'
            if not os.path.exists(kaggle_config_path):
                return Response({"error": "Kaggle credentials file not found"},
                                status=status.HTTP_400_BAD_REQUEST)

            with open(kaggle_config_path) as f:
                kaggle_auth = json.load(f)

            # Set Kaggle API credentials
            os.environ['KAGGLE_USERNAME'] = kaggle_auth.get('username', '')
            os.environ['KAGGLE_KEY'] = kaggle_auth.get('key', '')

            # Initialize and authenticate Kaggle API
            api = kaggle.KaggleApi()
            api.authenticate()

            # Create temp directory for metadata
            os.makedirs(kaggle_temp_path, exist_ok=True)
            kaggle_metadata_path = os.path.join(kaggle_temp_path, f"{dataset_ref.replace('/', '-')}")
            os.makedirs(kaggle_metadata_path, exist_ok=True)

            # Get dataset metadata
            api.dataset_metadata(dataset_ref, path=kaggle_metadata_path)
            metadata_file = os.path.join(kaggle_metadata_path, 'dataset-metadata.json')

            # Enhanced metadata parsing
            with open(metadata_file, "r") as file:
                metadata_content = file.read().strip()

                try:
                    # First try to parse as regular JSON
                    metadata = json.loads(metadata_content)

                    # If the result is a string, it might be doubly-encoded
                    if isinstance(metadata, str):
                        try:
                            # Parse the inner JSON
                            metadata = json.loads(metadata)
                        except json.JSONDecodeError as inner_error:
                            # If that fails, try cleaning the inner string
                            cleaned_inner = metadata.replace('\\"', '"')
                            metadata = json.loads(cleaned_inner)
                except json.JSONDecodeError as outer_error:
                    # If initial parse fails, try cleaning the outer string
                    try:
                        cleaned_outer = metadata_content.replace('\\"', '"')
                        metadata = json.loads(cleaned_outer)

                        # Check if we need to parse again
                        if isinstance(metadata, str):
                            metadata = json.loads(metadata)
                    except json.JSONDecodeError:
                        return Response({
                            "error": "Failed to parse doubly-encoded metadata",
                            "content_sample": metadata_content[:200] + "..." if len(
                                metadata_content) > 200 else metadata_content,
                            "parsing_steps": [
                                "1. Tried direct JSON parse",
                                "2. Tried cleaning outer JSON",
                                "3. Tried parsing inner JSON"
                            ]
                        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

                # Verify we got a dictionary
            if not isinstance(metadata, dict):
                return Response({
                    "error": "Final metadata is not a dictionary",
                    "metadata_type": str(type(metadata)),
                    "metadata_content": str(metadata)[:200] + "..." if len(str(metadata)) > 200 else str(metadata)
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            # Get dataset files
            files = api.dataset_list_files(dataset_ref)

            # Prepare download path
            hashed_name = hashlib.sha256(f"{dataset_ref}".encode()).hexdigest()[:16]
            download_path_kaggle = f"downloads/kaggle/{hashed_name}"
            os.makedirs(default_storage.path(download_path_kaggle), exist_ok=True)

            # Download dataset files
            # api.dataset_download_files(dataset_ref, path=default_storage.path(download_path_kaggle), unzip=True)

            # Prepare response data with proper defaults
            dataset_details = {
                "title": metadata.get("title", "No title available"),
                "datasetId": metadata.get("datasetId"),
                "datasetSlug": metadata.get("datasetSlug", ""),
                "description": metadata.get("description", ""),
                "owner": metadata.get("ownerUser", "Unknown owner"),
                "usabilityRating": metadata.get("usabilityRating", 0),
                "totalViews": metadata.get("totalViews", 0),
                "totalVotes": metadata.get("totalVotes", 0),
                "totalDownloads": metadata.get("totalDownloads", 0),
                "files": [file.name for file in files.files],
                "download_path": download_path_kaggle,
                "keywords": ", ".join([keyword for keyword in metadata.get("keywords", [])]),
                "licenses": ", ".join([lic.get("name", "") for lic in metadata.get("licenses", [])])
            }

            # Clean up
            if os.path.exists(metadata_file):
                os.remove(metadata_file)
            if os.path.exists(kaggle_metadata_path):
                try:
                    os.rmdir(kaggle_metadata_path)
                except OSError:
                    pass  # Directory not empty

            return Response({"dataset": dataset_details}, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({
                "error": "Unexpected error",
                "details": str(e),
                "type": type(e).__name__
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# get kaggle dataset list info
class KaggleDatasetsListView(APIView):
    def get(self, request):
        try:
            page = request.GET.get('page')
            # Load Kaggle credentials from JSON
            kaggle_config_path = 'config/kaggle.json'
            if not os.path.exists(kaggle_config_path):
                return Response({"error": "Kaggle credentials file not found"}, status=status.HTTP_400_BAD_REQUEST)

            with open(kaggle_config_path) as f:
                kaggle_auth = json.load(f)

            # Set Kaggle API credentials
            os.environ['KAGGLE_USERNAME'] = kaggle_auth.get('username', '')
            os.environ['KAGGLE_KEY'] = kaggle_auth.get('key', '')

            # Authenticate Kaggle API
            kaggle.api.authenticate()

            # Fetch datasets with pagination
            datasets = kaggle.api.dataset_list(page=int(page))  # Adjust page number as needed
            dataset_details = []

            # Extract key details from each dataset
            for dataset in datasets:
                details = {
                    "id": dataset.id if hasattr(dataset, 'id') else None,
                    "ref": dataset.ref if hasattr(dataset, 'ref') else None,
                    "title": dataset.title if hasattr(dataset, 'title') else None,
                    "subtitle": dataset.subtitle if hasattr(dataset, 'subtitle') else None,
                    "url": dataset.url if hasattr(dataset, 'url') else None,
                    "tags": [tag.name for tag in dataset.tags] if hasattr(dataset, 'tags') else [],
                }
                dataset_details.append(details)

            return Response({"datasets": dataset_details}, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)



# Cloud storage configuration
ACCOUNT = 'AUTH_aiahr-ae5aa48e'
AUTH_TOKEN = '391af3cea0e0248b92ad2d2671d4eaa8669854be'
STORAGE_BASE_URL = f'https://storage.aiahura.com/v1/{ACCOUNT}/'

# get kaggle dataset info
# download dataset files
# upload file to cloud storage
class UploadStorageKaggleView(APIView):
    def get(self, request):
        kaggle_temp_path = "./temp/kaggle/"
        dataset_ref = request.GET.get('dataset_ref')

        if not dataset_ref:
            return Response({"error": "dataset_ref parameter is required"},
                            status=status.HTTP_400_BAD_REQUEST)

        try:
            # Check and load Kaggle credentials
            kaggle_config_path = 'config/kaggle.json'
            if not os.path.exists(kaggle_config_path):
                return Response({"error": "Kaggle credentials file not found"},
                                status=status.HTTP_400_BAD_REQUEST)

            with open(kaggle_config_path) as f:
                kaggle_auth = json.load(f)

            # Set Kaggle API credentials
            os.environ['KAGGLE_USERNAME'] = kaggle_auth.get('username', '')
            os.environ['KAGGLE_KEY'] = kaggle_auth.get('key', '')

            # Initialize and authenticate Kaggle API
            api = kaggle.KaggleApi()
            api.authenticate()

            # Create temp directory for metadata
            os.makedirs(kaggle_temp_path, exist_ok=True)
            kaggle_metadata_path = os.path.join(kaggle_temp_path, f"{dataset_ref.replace('/', '-')}")
            os.makedirs(kaggle_metadata_path, exist_ok=True)

            # Get dataset metadata
            api.dataset_metadata(dataset_ref, path=kaggle_metadata_path)
            metadata_file = os.path.join(kaggle_metadata_path, 'dataset-metadata.json')

            # Enhanced metadata parsing
            with open(metadata_file, "r") as file:
                metadata_content = file.read().strip()

                try:
                    # First try to parse as regular JSON
                    metadata = json.loads(metadata_content)

                    # If the result is a string, it might be doubly-encoded
                    if isinstance(metadata, str):
                        try:
                            # Parse the inner JSON
                            metadata = json.loads(metadata)
                        except json.JSONDecodeError as inner_error:
                            # If that fails, try cleaning the inner string
                            cleaned_inner = metadata.replace('\\"', '"')
                            metadata = json.loads(cleaned_inner)
                except json.JSONDecodeError as outer_error:
                    # If initial parse fails, try cleaning the outer string
                    try:
                        cleaned_outer = metadata_content.replace('\\"', '"')
                        metadata = json.loads(cleaned_outer)

                        # Check if we need to parse again
                        if isinstance(metadata, str):
                            metadata = json.loads(metadata)
                    except json.JSONDecodeError:
                        return Response({
                            "error": "Failed to parse doubly-encoded metadata",
                            "content_sample": metadata_content[:200] + "..." if len(
                                metadata_content) > 200 else metadata_content,
                            "parsing_steps": [
                                "1. Tried direct JSON parse",
                                "2. Tried cleaning outer JSON",
                                "3. Tried parsing inner JSON"
                            ]
                        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

                # Verify we got a dictionary
            if not isinstance(metadata, dict):
                return Response({
                    "error": "Final metadata is not a dictionary",
                    "metadata_type": str(type(metadata)),
                    "metadata_content": str(metadata)[:200] + "..." if len(str(metadata)) > 200 else str(metadata)
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            # Get dataset files
            files = api.dataset_list_files(dataset_ref)

            # Prepare download path
            hashed_name = hashlib.sha256(f"{dataset_ref}".encode()).hexdigest()[:16]
            download_path_kaggle = f"downloads/kaggle/{hashed_name}"
            os.makedirs(default_storage.path(download_path_kaggle), exist_ok=True)

            # Download dataset files
            api.dataset_download_files(dataset_ref, path=default_storage.path(download_path_kaggle), unzip=True)
            # Prepare response data with proper defaults

            # Generate container name from dataset ID
            container_name = hashlib.sha256(str(metadata.get("datasetId")).encode()).hexdigest()[:16]

            # Create container in cloud storage
            container_url = f'{STORAGE_BASE_URL}{container_name}'
            headers = {
                'X-Auth-Token': AUTH_TOKEN
            }
            response = requests.put(container_url, headers=headers, verify=False)

            if response.status_code not in [201, 202]:
                return Response({
                    "error": "Failed to create storage container",
                    "details": response.text
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            # Create temporary directory for download
            with tempfile.TemporaryDirectory() as temp_dir:
                # Download dataset files to temp directory
                api.dataset_download_files(dataset_ref, path=temp_dir, unzip=True)

                # Upload all files to cloud storage
                file_urls = []
                for root, _, files in os.walk(temp_dir):
                    for file_name in files:
                        file_path = os.path.join(root, file_name)

                        # Read file content
                        with open(file_path, 'rb') as f:
                            file_data = f.read()

                        # Upload to cloud storage
                        file_url = f'{STORAGE_BASE_URL}{container_name}/{file_name}'
                        headers = {
                            'X-Auth-Token': AUTH_TOKEN,
                            'Content-Type': 'application/octet-stream'
                        }
                        upload_response = requests.put(file_url, headers=headers, data=file_data)

                        if upload_response.status_code in [201, 202]:
                            file_urls.append(file_url)
                        else:
                            return Response({
                                "error": f"Failed to upload file {file_name}",
                                "details": upload_response.text
                            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            # Prepare response data with cloud storage URLs
            dataset_details = {
                "title": metadata.get("title", "No title available"),
                "datasetId": metadata.get("datasetId"),
                "datasetSlug": metadata.get("datasetSlug", ""),
                "description": metadata.get("description", ""),
                "owner": metadata.get("ownerUser", "Unknown owner"),
                "usabilityRating": metadata.get("usabilityRating", 0),
                "totalViews": metadata.get("totalViews", 0),
                "totalVotes": metadata.get("totalVotes", 0),
                "totalDownloads": metadata.get("totalDownloads", 0),
                "files": file_urls,
                "container_name": container_name,
                "keywords": ", ".join([keyword for keyword in metadata.get("keywords", [])]),
                "licenses": ", ".join([lic.get("name", "") for lic in metadata.get("licenses", [])])
            }

            # Clean up local metadata file if it exists
            if os.path.exists(metadata_file):
                os.remove(metadata_file)
            if os.path.exists(kaggle_metadata_path):
                try:
                    os.rmdir(kaggle_metadata_path)
                except OSError:
                    pass  # Directory not empty

            return Response({"dataset": dataset_details}, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({
                "error": "Unexpected error",
                "details": str(e),
                "type": type(e).__name__
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)



# get kaggle dataset info
# download dataset files
# upload file to cloud storage
# update InternationalDataset table in database (change dataset_status= Download_Completed & signal update dataset table in database)
class TransferKaggleDatasetView(APIView):
    def upload_storage_kaggle1(self, dataset_ref):
        kaggle_temp_path = "./temp/kaggle/"

        if not dataset_ref:
            return {"error": "dataset_ref parameter is required"}, status.HTTP_400_BAD_REQUEST

        try:
            # Check and load Kaggle credentials
            kaggle_config_path = 'config/kaggle.json'
            if not os.path.exists(kaggle_config_path):
                return {"error": "Kaggle credentials file not found"}, status.HTTP_400_BAD_REQUEST

            with open(kaggle_config_path) as f:
                kaggle_auth = json.load(f)

            # Set Kaggle API credentials
            os.environ['KAGGLE_USERNAME'] = kaggle_auth.get('username', '')
            os.environ['KAGGLE_KEY'] = kaggle_auth.get('key', '')

            # Initialize and authenticate Kaggle API
            api = kaggle.KaggleApi()
            api.authenticate()

            # Create temp directory for metadata
            os.makedirs(kaggle_temp_path, exist_ok=True)
            kaggle_metadata_path = os.path.join(kaggle_temp_path, f"{dataset_ref.replace('/', '-')}")
            os.makedirs(kaggle_metadata_path, exist_ok=True)

            # Get dataset metadata
            api.dataset_metadata(dataset_ref, path=kaggle_metadata_path)
            metadata_file = os.path.join(kaggle_metadata_path, 'dataset-metadata.json')

            # Enhanced metadata parsing
            with open(metadata_file, "r") as file:
                metadata_content = file.read().strip()

                try:
                    metadata = json.loads(metadata_content)
                    if isinstance(metadata, str):
                        metadata = json.loads(metadata)
                except json.JSONDecodeError:
                    try:
                        cleaned_content = metadata_content.replace('\\"', '"')
                        metadata = json.loads(cleaned_content)
                        if isinstance(metadata, str):
                            metadata = json.loads(metadata)
                    except json.JSONDecodeError:
                        return {
                            "error": "Failed to parse metadata",
                            "content_sample": metadata_content[:200] + "..." if len(
                                metadata_content) > 200 else metadata_content
                        }, status.HTTP_500_INTERNAL_SERVER_ERROR

            if not isinstance(metadata, dict):
                return {
                    "error": "Invalid metadata format",
                    "metadata_type": str(type(metadata))
                }, status.HTTP_500_INTERNAL_SERVER_ERROR

            # Get dataset files
            files = api.dataset_list_files(dataset_ref)

            # Generate container name
            container_name = hashlib.sha256(str(metadata.get("datasetId")).encode()).hexdigest()[:16]

            # Create container in cloud storage
            container_url = f'{STORAGE_BASE_URL}{container_name}'
            headers = {'X-Auth-Token': AUTH_TOKEN}
            response = requests.put(container_url, headers=headers, verify=False)

            if response.status_code not in [201, 202]:
                return {
                    "error": "Failed to create storage container",
                    "details": response.text
                }, status.HTTP_500_INTERNAL_SERVER_ERROR

            # Download and upload files
            file_urls = []
            with tempfile.TemporaryDirectory() as temp_dir:
                api.dataset_download_files(dataset_ref, path=temp_dir, unzip=True)

                for root, _, files in os.walk(temp_dir):
                    for file_name in files:
                        file_path = os.path.join(root, file_name)
                        with open(file_path, 'rb') as f:
                            file_url = f'{STORAGE_BASE_URL}{container_name}/{file_name}'
                            upload_response = requests.put(
                                file_url,
                                headers={'X-Auth-Token': AUTH_TOKEN, 'Content-Type': 'application/octet-stream'},
                                data=f
                            )

                            if upload_response.status_code not in [201, 202]:
                                return {
                                    "error": f"Failed to upload file {file_name}",
                                    "details": upload_response.text
                                }, status.HTTP_500_INTERNAL_SERVER_ERROR
                            file_urls.append(file_url)

                            # Prepare response

                        dataset_details = {
                            "title": metadata.get("title", ""),
                            "datasetId": metadata.get("datasetId"),
                            "description": metadata.get("description", ""),
                            "owner": metadata.get("ownerUser", ""),
                            "totalViews": metadata.get("totalViews", 0),
                            "totalDownloads": metadata.get("totalDownloads", 0),
                            "files": file_urls,
                            "keywords": ", ".join(metadata.get("keywords", [])),
                            "licenses": ", ".join([lic.get("name", "") for lic in metadata.get("licenses", [])])
                        }

                        # Cleanup
                        if os.path.exists(metadata_file):
                            os.remove(metadata_file)
                        if os.path.exists(kaggle_metadata_path):
                            try:
                                os.rmdir(kaggle_metadata_path)
                            except OSError:
                                pass

                        return dataset_details, status.HTTP_200_OK

        except Exception as e:
            return {
                "error": "Unexpected error",
                "details": str(e)
            }, status.HTTP_500_INTERNAL_SERVER_ERROR

    def upload_storage_kaggle(self, dataset_ref):
        kaggle_temp_path = "./temp/kaggle/"

        if not dataset_ref:
            return {"error": "dataset_ref parameter is required"}, status.HTTP_400_BAD_REQUEST

        try:
            # Check and load Kaggle credentials
            kaggle_config_path = 'config/kaggle.json'
            if not os.path.exists(kaggle_config_path):
                return {"error": "Kaggle credentials file not found"}, status.HTTP_400_BAD_REQUEST

            with open(kaggle_config_path) as f:
                kaggle_auth = json.load(f)

            # Set Kaggle API credentials
            os.environ['KAGGLE_USERNAME'] = kaggle_auth.get('username', '')
            os.environ['KAGGLE_KEY'] = kaggle_auth.get('key', '')

            # Initialize and authenticate Kaggle API
            api = kaggle.KaggleApi()
            api.authenticate()

            # Create temp directory for metadata
            os.makedirs(kaggle_temp_path, exist_ok=True)
            kaggle_metadata_path = os.path.join(kaggle_temp_path, f"{dataset_ref.replace('/', '-')}")
            os.makedirs(kaggle_metadata_path, exist_ok=True)

            # Get dataset metadata
            api.dataset_metadata(dataset_ref, path=kaggle_metadata_path)
            metadata_file = os.path.join(kaggle_metadata_path, 'dataset-metadata.json')

            # Enhanced metadata parsing
            with open(metadata_file, "r") as file:
                metadata_content = file.read().strip()

                try:
                    metadata = json.loads(metadata_content)
                    if isinstance(metadata, str):
                        metadata = json.loads(metadata)
                except json.JSONDecodeError:
                    try:
                        cleaned_content = metadata_content.replace('\\"', '"')
                        metadata = json.loads(cleaned_content)
                        if isinstance(metadata, str):
                            metadata = json.loads(metadata)
                    except json.JSONDecodeError:
                        return {
                            "error": "Failed to parse metadata",
                            "content_sample": metadata_content[:200] + "..." if len(
                                metadata_content) > 200 else metadata_content
                        }, status.HTTP_500_INTERNAL_SERVER_ERROR

            if not isinstance(metadata, dict):
                return {
                    "error": "Invalid metadata format",
                    "metadata_type": str(type(metadata))
                }, status.HTTP_500_INTERNAL_SERVER_ERROR

            # Get dataset files
            files = api.dataset_list_files(dataset_ref)

            # Generate container name
            container_name = hashlib.sha256(str(metadata.get("datasetId")).encode()).hexdigest()[:16]

            # Create container in cloud storage
            container_url = f'{STORAGE_BASE_URL}{container_name}'
            headers = {'X-Auth-Token': AUTH_TOKEN}
            response = requests.put(container_url, headers=headers, verify=False)

            if response.status_code not in [201, 202]:
                return {
                    "error": "Failed to create storage container",
                    "details": response.text
                }, status.HTTP_500_INTERNAL_SERVER_ERROR

            # Download and upload files
            file_urls = []
            total_file_size = 0
            total_files_count = 0
            ref_link = f'https://www.kaggle.com/datasets/{metadata.get("ownerUser", "")}/{dataset_ref}'
            with tempfile.TemporaryDirectory() as temp_dir:
                api.dataset_download_files(dataset_ref, path=temp_dir, unzip=True)

                for root, _, files in os.walk(temp_dir):
                    for file_name in files:
                        file_path = os.path.join(root, file_name)
                        file_size = os.path.getsize(file_path)  # Get file size in bytes
                        total_file_size = total_file_size + file_size
                        total_files_count = total_files_count + 1
                        with open(file_path, 'rb') as f:
                            file_url = f'{STORAGE_BASE_URL}{container_name}/{file_name}'
                            upload_response = requests.put(
                                file_url,
                                headers={'X-Auth-Token': AUTH_TOKEN, 'Content-Type': 'application/octet-stream'},
                                data=f
                            )
                if upload_response.status_code not in [201, 202]:
                    return {
                        "error": f"Failed to upload file {file_name}", "details": upload_response.text
                    }, status.HTTP_500_INTERNAL_SERVER_ERROR

                    # Store both URL and size information
                file_urls.append({
                    "url": file_url,
                    "size": file_size,
                    "size_human": f"{file_size / 1024:.2f} KB" if file_size < 1024 * 1024 else f"{file_size / (1024 * 1024):.2f} MB"
                })

                # Prepare response
                dataset_details = {
                    "title": metadata.get("title", ""),
                    "datasetId": metadata.get("datasetId"),
                    "description": metadata.get("description", ""),
                    "owner": metadata.get("ownerUser", ""),
                    "totalViews": metadata.get("totalViews", 0),
                    "totalDownloads": metadata.get("totalDownloads", 0),
                    "files": file_urls,
                    "size": total_file_size,
                    "filesCount": total_files_count,
                    "refLink": ref_link,
                    "keywords": ", ".join(metadata.get("keywords", [])),
                    "licenses": ", ".join([lic.get("name", "") for lic in metadata.get("licenses", [])])
                }

                # Cleanup
                if os.path.exists(metadata_file):
                    os.remove(metadata_file)
                if os.path.exists(kaggle_metadata_path):
                    try:
                        os.rmdir(kaggle_metadata_path)
                    except OSError:
                        pass

                return dataset_details, status.HTTP_200_OK

        except Exception as e:
            return {
                "error": "Unexpected error",
                "details": str(e)
            }, status.HTTP_500_INTERNAL_SERVER_ERROR


    def get(self, request):
        dataset_ref = request.GET.get('dataset_ref')
        if not dataset_ref:
            return Response({"error": "dataset_ref parameter is required"},
                            status=status.HTTP_400_BAD_REQUEST)

        try:
            # Get dataset details from Kaggle and upload to storage
            dataset_details, status_code = self.upload_storage_kaggle(dataset_ref)
            if status_code != status.HTTP_200_OK:
                return Response(dataset_details, status=status_code)

            # Update InternationalDataset
            try:
                international_dataset = InternationalDataset.objects.get(internalCode=dataset_ref)

                with transaction.atomic():
                    international_dataset.owner = dataset_details.get('owner', '')
                    international_dataset.internalId = str(dataset_details.get('datasetId', ''))
                    international_dataset.desc = dataset_details.get('description', '')
                    international_dataset.license = dataset_details.get('licenses', '')
                    international_dataset.dataset_tags = dataset_details.get('keywords', '')
                    international_dataset.likes = dataset_details.get('totalViews', 0)
                    international_dataset.downloads = dataset_details.get('totalDownloads', 0)
                    international_dataset.size = dataset_details.get('size')
                    international_dataset.downloadLink = dataset_details.get('files', [])
                    international_dataset.filesCount = dataset_details.get('filesCount')
                    international_dataset.refLink = dataset_details.get('refLink')
                    international_dataset.dataset_status = 'Download_Completed'

                    international_dataset.save()
                return Response({
                    "status": "success",
                    "dataset_id": international_dataset.id
                }, status=status.HTTP_200_OK)

            except InternationalDataset.DoesNotExist:
                return Response({"error": "Dataset not found"}, status=status.HTTP_404_NOT_FOUND)

        except Exception as e:
            return Response({
                "error": "Unexpected error",
                "details": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# get kaggle list datasets
# insert ref of dataset to InternationalDataset table in database
class BulkImportKaggleView(APIView):
    permission_classes = [IsAuthenticated, BlocklistPermission]
    def get(self, request):
        page = request.GET.get('page')
        num_added_records = 0
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

            # Authenticate Kaggle API
            kaggle.api.authenticate()

            # Fetch datasets with pagination
            datasets = kaggle.api.dataset_list(page=int(page))  # Adjust page number as needed

            for dataset in datasets:
                try:
                    result = {
                        "name": dataset.title if hasattr(dataset, 'title') else None,
                        # "owner": data.get('author'),
                        "internalId": dataset.id if hasattr(dataset, 'id') else None,
                        "internalCode": dataset.ref if hasattr(dataset, 'ref') else None,
                        # "language":
                        # "desc":
                        # "license":
                        # "tasks":
                        # "datasetDate":
                        # "size":
                        # "format":
                        # "dataType":
                        # "columnDataType":
                        "dataset_tags": ', '.join([tag.name for tag in dataset.tags]),
                        # "likes":
                        # "downloads":
                        "referenceOwner": 'Kaggle',
                        # "sourceJson":
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
                    print(f"Error processing dataset {dataset.get('id', 'Unknown')}: {e}")

        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response({"response": f"{num_added_records} Records Added"}, status=status.HTTP_201_CREATED)

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

