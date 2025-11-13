from django.db.models import Q
from rest_framework.pagination import PageNumberPagination
from django.shortcuts import render
from rest_framework.decorators import api_view
from rest_framework.response import Response
from django.http import JsonResponse
from rest_framework.views import APIView
import requests
from rest_framework import status
# from rest_framework.authentication import TokenAuthentication
from rest_framework.permissions import IsAuthenticated, IsAdminUser
from django.contrib.auth.models import User
from .serializers import UserSerializer, UserDetailSerializer, UserFullDetailSerializer, DatasetSerializer, \
    InternationalDatasetSerializer, CommentSerializer
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
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError


##################################################
# S3-Compatible Storage Configuration
##################################################

def get_s3_client():
    """Get S3 client with enhanced configuration for S3-compatible storage"""
    return boto3.client(
        's3',
        endpoint_url=settings.CLOUD_STORAGE_CONFIG['S3_ENDPOINT'],
        aws_access_key_id=settings.CLOUD_STORAGE_CONFIG['ACCESS_KEY'],
        aws_secret_access_key=settings.CLOUD_STORAGE_CONFIG['SECRET_KEY'],
        region_name=settings.CLOUD_STORAGE_CONFIG.get('REGION', 'us-east-1'),
        config=Config(
            signature_version='s3v4',
            s3={'addressing_style': 'virtual'},
            retries={
                'max_attempts': 5,
                'mode': 'adaptive'
            },
            max_pool_connections=50,
            proxies={} if not settings.CLOUD_STORAGE_CONFIG.get('PROXY') else {
                'http': settings.CLOUD_STORAGE_CONFIG['PROXY'],
                'https': settings.CLOUD_STORAGE_CONFIG['PROXY']
            }
        ),
        verify=settings.CLOUD_STORAGE_CONFIG.get('VERIFY_SSL', False)
    )


def create_s3_bucket(bucket_name):
    """Create a bucket in S3-compatible storage"""
    s3_client = get_s3_client()
    try:
        # Check if bucket already exists
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            return True, bucket_name
        except ClientError:
            # Bucket doesn't exist, create it
            pass

        # Create bucket
        s3_client.create_bucket(Bucket=bucket_name)

        # Wait for bucket to be ready
        import time
        time.sleep(1)

        return True, bucket_name
    except Exception as e:
        return False, str(e)


def sizeof_fmt(num, suffix='B'):
    """Convert file size to human readable format"""
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return f"{num:3.1f} {unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f} Y{suffix}"


def get_content_type(filename):
    """Get content type based on file extension"""
    ext = os.path.splitext(filename)[1].lower()
    content_types = {
        '.csv': 'text/csv',
        '.parquet': 'application/octet-stream',
        '.json': 'application/json',
        '.txt': 'text/plain',
        '.zip': 'application/zip',
        '.rar': 'application/x-rar-compressed',
        '.pdf': 'application/pdf',
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.png': 'image/png',
    }
    return content_types.get(ext, 'application/octet-stream')


def upload_via_requests(file_path, bucket_name, file_name):
    """Upload using direct HTTP requests - most reliable for MinIO"""
    try:
        # Read file content
        with open(file_path, 'rb') as f:
            file_content = f.read()

        print(f"Uploading {file_name} via requests ({len(file_content)} bytes)")

        # Generate presigned URL for PUT
        s3_client = get_s3_client()
        presigned_url = s3_client.generate_presigned_url(
            'put_object',
            Params={
                'Bucket': bucket_name,
                'Key': file_name,
                'ContentType': get_content_type(file_name)
            },
            ExpiresIn=3600
        )

        # Upload using requests
        response = requests.put(
            presigned_url,
            data=file_content,
            headers={'Content-Type': get_content_type(file_name)},
            verify=False
        )

        if response.status_code in [200, 204]:
            print(f"Successfully uploaded via requests: {file_name}")
            return True, {
                "bucket_name": bucket_name,
                "object_key": file_name,
                "size": len(file_content),
                "size_human": sizeof_fmt(len(file_content))
            }
        else:
            error_msg = f"HTTP {response.status_code}: {response.text}"
            print(f"Requests upload failed: {error_msg}")
            return False, error_msg

    except Exception as e:
        error_msg = f"Requests upload failed: {str(e)}"
        print(error_msg)
        return False, error_msg


def upload_to_s3(file_path, bucket_name, object_key):
    """Upload file using upload_file method with enhanced error handling"""
    s3_client = get_s3_client()
    try:
        file_size = os.path.getsize(file_path)
        print(f"File size: {file_size} bytes")

        # Upload with multipart for larger files
        if file_size > 10 * 1024 * 1024:  # 10MB
            print("Using multipart upload for large file")
            config = boto3.s3.transfer.TransferConfig(
                multipart_threshold=10 * 1024 * 1024,
                max_concurrency=10,
                multipart_chunksize=8 * 1024 * 1024,
                use_threads=True
            )

            s3_client.upload_file(
                file_path,
                bucket_name,
                object_key,
                Config=config,
                ExtraArgs={
                    'ContentType': 'application/octet-stream',
                    'ContentDisposition': f'attachment; filename="{os.path.basename(object_key)}"'
                }
            )
        else:
            # For smaller files, use standard upload
            s3_client.upload_file(
                file_path,
                bucket_name,
                object_key,
                ExtraArgs={
                    'ContentType': 'application/octet-stream',
                    'ContentDisposition': f'attachment; filename="{os.path.basename(object_key)}"'
                }
            )

        # Verify upload by checking if object exists
        try:
            response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
            uploaded_size = response['ContentLength']
            print(f"Uploaded file size: {uploaded_size} bytes")

            return True, {
                "bucket_name": bucket_name,
                "object_key": object_key,
                "size": uploaded_size,
                "size_human": sizeof_fmt(uploaded_size)
            }

        except Exception as e:
            print(f"Could not verify upload: {str(e)}")
            return False, f"Upload verification failed: {str(e)}"

    except Exception as e:
        error_msg = f"S3 Upload File Error for {object_key}: {str(e)}"
        print(error_msg)
        return False, error_msg


def upload_http_response_to_s3(http_response, bucket_name, file_name):
    """Upload HTTP response directly to S3 without saving to disk"""
    try:
        # Read the content from HTTP response
        file_content = http_response.content
        file_size = len(file_content)
        print(f"Uploading {file_name} from HTTP response ({file_size} bytes)")

        # Generate presigned URL for PUT
        s3_client = get_s3_client()
        presigned_url = s3_client.generate_presigned_url(
            'put_object',
            Params={
                'Bucket': bucket_name,
                'Key': file_name,
                'ContentType': get_content_type(file_name)
            },
            ExpiresIn=3600
        )

        # Upload using requests
        response = requests.put(
            presigned_url,
            data=file_content,
            headers={'Content-Type': get_content_type(file_name)},
            verify=False
        )

        if response.status_code in [200, 204]:
            print(f"Successfully uploaded via requests: {file_name}")
            return True, {
                "bucket_name": bucket_name,
                "object_key": file_name,
                "size": file_size,
                "size_human": sizeof_fmt(file_size)
            }
        else:
            error_msg = f"HTTP {response.status_code}: {response.text}"
            print(f"Requests upload failed: {error_msg}")
            return False, error_msg

    except Exception as e:
        error_msg = f"HTTP response upload failed: {str(e)}"
        print(error_msg)
        return False, error_msg


def upload_with_fallback(file_path_or_response, bucket_name, file_name, is_http_response=False):
    """Try multiple upload methods with fallback"""
    if is_http_response:
        # Handle HTTP response objects
        methods = [
            ("Direct HTTP Requests", upload_http_response_to_s3),
        ]
    else:
        # Handle file paths
        methods = [
            ("Direct HTTP Requests", upload_via_requests),
            ("S3 upload_file", upload_to_s3),
        ]

    for method_name, upload_func in methods:
        print(f"Trying {method_name} for {file_name}")
        success, result = upload_func(file_path_or_response, bucket_name, file_name)
        if success:
            print(f"Success with {method_name}")
            return True, result
        else:
            print(f"Failed with {method_name}: {result}")

    return False, "All upload methods failed"


def upload_data_to_s3(file_data, bucket_name, object_key, content_type='application/octet-stream'):
    """Upload file data directly to S3-compatible storage"""
    s3_client = get_s3_client()
    try:
        file_size = len(file_data)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=file_data,
            ContentType=content_type,
            ContentDisposition=f'attachment; filename="{os.path.basename(object_key)}"'
        )

        return True, {
            "bucket_name": bucket_name,
            "object_key": object_key,
            "size": file_size,
            "size_human": sizeof_fmt(file_size)
        }
    except Exception as e:
        return False, str(e)


def get_file_size_from_s3(bucket_name, object_key):
    """Get file size from S3 object"""
    s3_client = get_s3_client()
    try:
        response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
        return True, response['ContentLength']
    except Exception as e:
        return False, str(e)


def generate_presigned_url(bucket_name, object_key, expiration=3600):
    """Generate presigned URL for temporary access to S3 object"""
    s3_client = get_s3_client()
    try:
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': bucket_name,
                'Key': object_key
            },
            ExpiresIn=expiration
        )
        return True, url
    except Exception as e:
        return False, str(e)


class StandardResultsSetPagination(PageNumberPagination):
    page_size = 1
    page_size_query_param = 'page_size'
    max_page_size = 50


##################################################
# General
##################################################

##################################################
# Huggingface - S3 Compatible
##################################################

class HuggingfaceDatasetDetailView(APIView):
    def get(self, request):
        repo_id = request.GET.get('repo_id')
        response = requests.get(f"https://huggingface.co/api/datasets/{repo_id}/croissant")
        data = response.json()
        result = {
            "data": data
        }
        return Response(data=result)


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


class ImportHuggingfaceView(APIView):
    def post(self, request):
        res = requests.get(f"https://huggingface.co/api/datasets?full=full&limit=2")
        data = res.json()

        # Process the first dataset from the response
        dataset_data = data[0] if data else {}
        card_data = dataset_data.get('cardData', {})

        result = {
            "name": dataset_data.get('id'),
            "owner": dataset_data.get('author'),
            "internalId": dataset_data.get('_id'),
            "internalCode": dataset_data.get('id'),
            "language": card_data.get('language', [''])[0] if isinstance(card_data.get('language'),
                                                                         list) else card_data.get('language', ''),
            "desc": dataset_data.get('description'),
            "license": card_data.get('license', [''])[0] if isinstance(card_data.get('license'),
                                                                       list) else card_data.get('license', ''),
            "tasks": card_data.get('task_categories', [''])[0] if isinstance(card_data.get('task_categories'),
                                                                             list) else card_data.get('task_categories',
                                                                                                      ''),
            "datasetDate": dataset_data.get('createdAt'),
            "sourceJson": dataset_data,
        }

        ser = InternationalDatasetSerializer(data=result, context={'request': request})
        if ser.is_valid():
            ser.validated_data['user'] = request.user
            instance = ser.save()
            return Response({"response": "Added"}, status=status.HTTP_201_CREATED)
        return Response(ser.errors, status=400)


class BulkImportHuggingfaceView(APIView):
    permission_classes = [IsAuthenticated, BlocklistPermission]

    def post(self, request):
        page = request.GET.get('page')
        limit = request.GET.get('limit')
        res = requests.get(f"https://huggingface.co/api/datasets?full=full&limit={limit}")
        dataList = res.json()
        num_added_records = 0

        for data in dataList:
            try:
                card_data = data.get('cardData', {})
                dataset_info = card_data.get('dataset_info', {})

                dataset_format = ''
                if data.get('tags'):
                    dataset_format = next((tag.split(":")[1] for tag in data.get('tags') if tag.startswith("format:")),
                                          None)

                data_type = ''
                if data.get('tags'):
                    data_type = next((tag.split(":")[1] for tag in data.get('tags') if tag.startswith("modality:")),
                                     None)

                dataset_tags = ''
                if card_data.get('tags'):
                    dataset_tags = ', '.join(card_data.get('tags')) + ', ' + ', '.join(card_data.get('task_categories'))

                if isinstance(dataset_info, list) and dataset_info:
                    dataset_info = dataset_info[0]

                language = card_data.get('language')
                if isinstance(language, list):
                    language = ", ".join(language)

                result = {
                    "name": data.get('id'),
                    "owner": data.get('author'),
                    "internalId": data.get('_id'),
                    "internalCode": data.get('id'),
                    "language": language,
                    "desc": data.get('description'),
                    "license": card_data.get('license', [None])[0],
                    "tasks": card_data.get('task_categories', [None])[0],
                    "datasetDate": data.get('createdAt'),
                    "size": dataset_info.get('dataset_size'),
                    "format": dataset_format,
                    "dataType": data_type,
                    "columnDataType": dataset_info.get('features'),
                    "dataset_tags": dataset_tags,
                    "likes": data.get('likes'),
                    "downloads": data.get('downloads'),
                    "referenceOwner": 'HuggingFace',
                    "refLink": f"https://huggingface.co/datasets/{data.get('id')}'",
                    "sourceJson": data,
                }

                ser = InternationalDatasetSerializer(data=result, context={'request': request})
                if ser.is_valid():
                    ser.validated_data['user'] = request.user
                    ser.save()
                    num_added_records += 1
                else:
                    print(f"Validation error: {ser.errors}")

            except Exception as e:
                print(f"Error processing dataset {data.get('id', 'Unknown')}: {e}")

        return Response({"response": f"{num_added_records} Records Added"}, status=status.HTTP_201_CREATED)


class HuggingfaceParquetFilesView(APIView):

    async def fetch_and_upload_to_s3(self, session, url, bucket_name, object_key):
        """Asynchronously download and upload to S3"""
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    return {"error": f"Failed to download {url}"}

                content = await response.read()
                upload_success, upload_info = upload_data_to_s3(content, bucket_name, object_key)

                if upload_success:
                    return {
                        "success": True,
                        "upload_info": upload_info
                    }
                else:
                    return {"error": f"Failed to upload to S3: {upload_info}"}
        except Exception as e:
            return {"error": f"Exception while processing {url}: {str(e)}"}

    async def download_and_upload_files(self, json_data, repo_id):
        """Handles multiple concurrent downloads and S3 uploads"""
        download_links = []
        total_file_size = 0
        tasks = []

        # Create S3 bucket
        bucket_name = hashlib.sha256(repo_id.encode()).hexdigest()[:16]
        bucket_success, bucket_result = create_s3_bucket(bucket_name)
        if not bucket_success:
            return {"error": f"Failed to create S3 bucket: {bucket_result}"}

        async with aiohttp.ClientSession() as session:
            for dataset, categories in json_data.items():
                for category, urls in categories.items():
                    for url in urls:
                        file_name = os.path.basename(url)
                        object_key = f"{dataset}/{category}/{file_name}"

                        # Schedule async download and upload
                        task = self.fetch_and_upload_to_s3(session, url, bucket_name, object_key)
                        tasks.append(task)

            # Run tasks concurrently
            results = await asyncio.gather(*tasks)

            # Process results
            for result in results:
                if "success" in result and result["success"]:
                    upload_info = result["upload_info"]
                    download_link = {
                        "bucket_name": upload_info["bucket_name"],
                        "object_key": upload_info["object_key"],
                        "size": upload_info["size"],
                        "size_human": upload_info["size_human"]
                    }
                    download_links.append(download_link)
                    total_file_size += upload_info["size"]
                elif "error" in result:
                    print(f"Error: {result['error']}")

        return {
            "bucket_name": bucket_name,
            "download_links": download_links,
            "total_size": total_file_size,
            "file_count": len(download_links)
        }

    async def update_database(self, repo_id, dataset_status, download_links, bucket_name, total_size, file_count):
        try:
            dataset = await sync_to_async(InternationalDataset.objects.get)(name=repo_id)
            dataset.dataset_status = dataset_status
            dataset.downloadLink = download_links
            dataset.code = bucket_name
            dataset.size = total_size
            dataset.filesCount = file_count
            await sync_to_async(dataset.save)()

            # Create corresponding Dataset entry
            await self.create_dataset_entry(dataset)

        except InternationalDataset.DoesNotExist:
            return {"error": f"Repo with repo_id {repo_id} not found in the database."}

    async def create_dataset_entry(self, international_dataset):
        """Create a Dataset entry from InternationalDataset"""
        try:
            # Check if Dataset already exists
            existing_dataset = await sync_to_async(
                Dataset.objects.filter(name=international_dataset.name).first
            )()

            if existing_dataset:
                # Update existing dataset
                existing_dataset.owner = international_dataset.owner
                existing_dataset.desc = international_dataset.desc
                existing_dataset.license = international_dataset.license
                existing_dataset.size = international_dataset.size
                existing_dataset.downloadLink = international_dataset.downloadLink
                existing_dataset.filesCount = international_dataset.filesCount
                await sync_to_async(existing_dataset.save)()
                return existing_dataset
            else:
                # Create new dataset
                dataset_data = {
                    "name": international_dataset.name,
                    "owner": international_dataset.owner,
                    "desc": international_dataset.desc,
                    "license": international_dataset.license,
                    "size": international_dataset.size,
                    "downloadLink": international_dataset.downloadLink,
                    "filesCount": international_dataset.filesCount,
                    "user": international_dataset.user,
                    "international_dataset": international_dataset
                }

                ser = DatasetSerializer(data=dataset_data)
                if ser.is_valid():
                    dataset = await sync_to_async(ser.save)()
                    return dataset
                else:
                    print(f"Dataset validation error: {ser.errors}")
                    return None

        except Exception as e:
            print(f"Error creating Dataset entry: {str(e)}")
            return None

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

            # Download files and upload to S3
            upload_result = await self.download_and_upload_files(json_data, repo_id)
            if "error" in upload_result:
                return JsonResponse(upload_result, status=500)

            # Update database
            update_res = await self.update_database(
                repo_id,
                'Download_Completed',
                upload_result["download_links"],
                upload_result["bucket_name"],
                upload_result["total_size"],
                upload_result["file_count"]
            )

            return JsonResponse({
                "bucket_name": upload_result["bucket_name"],
                "download_links": upload_result["download_links"],
                "total_size": upload_result["total_size"],
                "file_count": upload_result["file_count"]
            })
        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)

    async def dispatch(self, request):
        """Handle async dispatch without asyncio.run()."""
        if asyncio.iscoroutinefunction(self.get):
            return await self.get(request)
        return await super().dispatch(request)


class TransferHuggingfaceDatasetView(APIView):
    def post(self, request):
        """Handle POST requests for transferring HuggingFace datasets"""
        dataset_ref = request.data.get('dataset_ref')

        if not dataset_ref:
            return Response(
                {"error": "dataset_ref parameter is required"},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Call your existing upload method
        result, status_code = self.upload_storage_huggingface(dataset_ref)
        return Response(result, status=status_code)

    def get(self, request):
        """Handle GET requests"""
        dataset_ref = request.GET.get('dataset_ref')
        if not dataset_ref:
            return Response(
                {"error": "dataset_ref parameter is required"},
                status=status.HTTP_400_BAD_REQUEST
            )

        result, status_code = self.upload_storage_huggingface(dataset_ref)
        return Response(result, status=status_code)

    def upload_storage_huggingface(self, dataset_ref):
        """Download HuggingFace dataset and upload to S3-compatible storage"""
        if not dataset_ref:
            return {"error": "dataset_ref parameter is required"}, status.HTTP_400_BAD_REQUEST

        try:
            # Get dataset metadata
            api_url = f"https://huggingface.co/api/datasets/{dataset_ref}"
            response = requests.get(api_url)
            if response.status_code != 200:
                return {
                           "error": "Failed to fetch dataset metadata",
                           "status_code": response.status_code
                       }, status.HTTP_500_INTERNAL_SERVER_ERROR
            metadata = response.json()

            # Get download URLs
            download_urls = []
            try:
                parquet_response = requests.get(f"https://huggingface.co/api/datasets/{dataset_ref}/parquet")
                if parquet_response.status_code == 200:
                    data = parquet_response.json()
                    for config in data.values():
                        if isinstance(config, dict):
                            for split in config.values():
                                if isinstance(split, list):
                                    download_urls.extend(split)
            except Exception as e:
                print(f"Parquet API error: {e}")

            # Fallback to files API
            ALLOWED_EXTENSIONS = {'.csv', '.parquet', '.zip', '.rar'}
            if not download_urls:
                files_response = requests.get(f"https://huggingface.co/api/datasets/{dataset_ref}/tree/main")
                if files_response.status_code == 200:
                    files_data = files_response.json()
                    download_urls = [
                        f"https://huggingface.co/datasets/{dataset_ref}/resolve/main/{f['path']}"
                        for f in files_data
                        if f['type'] == 'file' and
                           any(f['path'].lower().endswith(ext) for ext in ALLOWED_EXTENSIONS)
                    ]

            if not download_urls:
                return {
                           "error": "No downloadable files found",
                           "details": "Neither parquet nor regular files were accessible"
                       }, status.HTTP_400_BAD_REQUEST

            # Create S3 bucket
            bucket_name = hashlib.sha256(dataset_ref.encode()).hexdigest()[:16]
            bucket_success, bucket_result = create_s3_bucket(bucket_name)
            if not bucket_success:
                return {
                           "error": "Failed to create S3 bucket",
                           "details": bucket_result
                       }, status.HTTP_500_INTERNAL_SERVER_ERROR

            # Process files
            download_links = []
            total_size = 0

            with tempfile.TemporaryDirectory() as temp_dir:
                for file_url in download_urls:
                    try:
                        file_name = os.path.basename(file_url)
                        print(f"Downloading {file_name} from {file_url}")

                        # Download file to temporary directory
                        file_response = requests.get(file_url, stream=True)
                        if file_response.status_code != 200:
                            print(f"Failed to download {file_url}: Status {file_response.status_code}")
                            continue

                        # Save file to temporary directory
                        temp_file_path = os.path.join(temp_dir, file_name)
                        with open(temp_file_path, 'wb') as f:
                            for chunk in file_response.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)

                        # Get file size
                        file_size = os.path.getsize(temp_file_path)
                        print(f"Downloaded {file_name} ({file_size} bytes)")

                        # Upload to S3 storage using the fallback method
                        upload_success, upload_info = upload_with_fallback(
                            temp_file_path,
                            bucket_name,
                            file_name
                        )

                        if not upload_success:
                            print(f"Failed to upload {file_name}: {upload_info}")
                            continue

                        # Create download link in new format
                        download_link = {
                            "bucket_name": upload_info["bucket_name"],
                            "object_key": upload_info["object_key"],
                            "size": upload_info["size"],
                            "size_human": upload_info["size_human"]
                        }
                        download_links.append(download_link)
                        total_size += upload_info["size"]

                    except Exception as e:
                        print(f"Error processing {file_url}: {str(e)}")
                        continue

            if not download_links:
                return {
                           "error": "Failed to download/upload any files",
                           "details": "All file transfers failed"
                       }, status.HTTP_500_INTERNAL_SERVER_ERROR

            # Prepare response
            card_data = metadata.get('cardData', {})
            dataset_info = card_data.get('dataset_info', {})
            if isinstance(dataset_info, list) and dataset_info:
                dataset_info = dataset_info[0]

            language = card_data.get('language', '')
            if isinstance(language, list):
                language = ", ".join(language)

            result_data = {
                "code": bucket_name,
                "title": metadata.get('id', ''),
                "name": metadata.get('id', ''),
                "datasetId": metadata.get('_id', ''),
                "description": metadata.get('description', ''),
                "owner": metadata.get('author', ''),
                "totalViews": metadata.get('likes', 0),
                "totalDownloads": metadata.get('downloads', 0),
                "download_links": download_links,
                "size": total_size,
                "filesCount": len(download_links),
                "refLink": f"https://huggingface.co/datasets/{dataset_ref}",
                "keywords": ", ".join(card_data.get('tags', [])),
                "licenses": ", ".join(card_data.get('license', [])),
                "language": language,
                "referenceOwner": 'HuggingFace',
                "tasks": ", ".join(card_data.get('task_categories', []))
            }

            # Save to database
            try:
                self.save_to_databases(dataset_ref, result_data)
            except Exception as e:
                print(f"Error saving to database: {str(e)}")

            return result_data, status.HTTP_200_OK

        except Exception as e:
            return {
                       "error": "Unexpected error",
                       "details": str(e)
                   }, status.HTTP_500_INTERNAL_SERVER_ERROR

    def save_to_databases(self, dataset_ref, dataset_data):
        """Save dataset to both InternationalDataset and Dataset tables"""
        try:
            # Update or create InternationalDataset
            international_dataset, created = InternationalDataset.objects.get_or_create(
                internalCode=dataset_ref,
                defaults={
                    'name': dataset_data.get('name', ''),
                    'owner': dataset_data.get('owner', ''),
                    'internalId': str(dataset_data.get('datasetId', '')),
                    'desc': dataset_data.get('description', ''),
                    'license': dataset_data.get('licenses', ''),
                    'dataset_tags': dataset_data.get('keywords', ''),
                    'likes': dataset_data.get('totalViews', 0),
                    'downloads': dataset_data.get('totalDownloads', 0),
                    'size': dataset_data.get('size'),
                    'downloadLink': dataset_data.get('download_links', []),
                    'filesCount': dataset_data.get('filesCount'),
                    'refLink': dataset_data.get('refLink'),
                    'language': dataset_data.get('language', ''),
                    'tasks': dataset_data.get('tasks', ''),
                    'referenceOwner': dataset_data.get('referenceOwner', ''),
                    'dataset_status': 'Download_Completed',
                    'code': dataset_data.get('code', ''),
                    'price': 0.0
                }
            )

            if not created:
                # Update existing record
                international_dataset.name = dataset_data.get('name', '')
                international_dataset.owner = dataset_data.get('owner', '')
                international_dataset.desc = dataset_data.get('description', '')
                international_dataset.license = dataset_data.get('licenses', '')
                international_dataset.dataset_tags = dataset_data.get('keywords', '')
                international_dataset.likes = dataset_data.get('totalViews', 0)
                international_dataset.downloads = dataset_data.get('totalDownloads', 0)
                international_dataset.size = dataset_data.get('size')
                international_dataset.downloadLink = dataset_data.get('download_links', [])
                international_dataset.filesCount = dataset_data.get('filesCount')
                international_dataset.refLink = dataset_data.get('refLink')
                international_dataset.language = dataset_data.get('language', '')
                international_dataset.tasks = dataset_data.get('tasks', '')
                international_dataset.referenceOwner = dataset_data.get('referenceOwner', '')
                international_dataset.dataset_status = 'Download_Completed'
                international_dataset.code = dataset_data.get('code', '')
                international_dataset.price = 0.0
                international_dataset.save()

            # Create or update Dataset entry
            dataset, dataset_created = Dataset.objects.get_or_create(
                name=dataset_data.get('name', ''),
                defaults={
                    'owner': dataset_data.get('owner', ''),
                    'desc': dataset_data.get('description', ''),
                    'license': dataset_data.get('licenses', ''),
                    'size': dataset_data.get('size'),
                    'downloadLink': dataset_data.get('download_links', []),
                    'filesCount': dataset_data.get('filesCount'),
                    'international_dataset': international_dataset
                }
            )

            if not dataset_created:
                # Update existing dataset
                dataset.owner = dataset_data.get('owner', '')
                dataset.desc = dataset_data.get('description', '')
                dataset.license = dataset_data.get('licenses', '')
                dataset.size = dataset_data.get('size')
                dataset.downloadLink = dataset_data.get('download_links', [])
                dataset.filesCount = dataset_data.get('filesCount')
                dataset.international_dataset = international_dataset
                dataset.save()

            return international_dataset, dataset

        except Exception as e:
            print(f"Error saving to databases: {str(e)}")
            raise


##################################################
# Kaggle - S3 Compatible
##################################################

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
                        return Response({
                            "error": "Failed to parse metadata",
                            "content_sample": metadata_content[:200] + "..." if len(
                                metadata_content) > 200 else metadata_content,
                        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            if not isinstance(metadata, dict):
                return Response({
                    "error": "Final metadata is not a dictionary",
                    "metadata_type": str(type(metadata)),
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            # Get dataset files
            files = api.dataset_list_files(dataset_ref)

            # Prepare response data
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
                    pass

            return Response({"dataset": dataset_details}, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({
                "error": "Unexpected error",
                "details": str(e),
                "type": type(e).__name__
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class KaggleDatasetsListView(APIView):
    def get(self, request):
        try:
            page = request.GET.get('page')
            kaggle_config_path = 'config/kaggle.json'
            if not os.path.exists(kaggle_config_path):
                return Response({"error": "Kaggle credentials file not found"}, status=status.HTTP_400_BAD_REQUEST)

            with open(kaggle_config_path) as f:
                kaggle_auth = json.load(f)

            os.environ['KAGGLE_USERNAME'] = kaggle_auth.get('username', '')
            os.environ['KAGGLE_KEY'] = kaggle_auth.get('key', '')

            kaggle.api.authenticate()

            datasets = kaggle.api.dataset_list(page=int(page))
            dataset_details = []

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


class BulkImportKaggleView(APIView):
    permission_classes = [IsAuthenticated, BlocklistPermission]

    def get(self, request):
        page = request.GET.get('page')
        num_added_records = 0
        try:
            kaggle_config_path = 'config/kaggle.json'
            if not os.path.exists(kaggle_config_path):
                return Response({"error": "Kaggle credentials file not found"}, status=status.HTTP_400_BAD_REQUEST)

            with open(kaggle_config_path) as f:
                kaggle_auth = json.load(f)

            os.environ['KAGGLE_USERNAME'] = kaggle_auth.get('username', '')
            os.environ['KAGGLE_KEY'] = kaggle_auth.get('key', '')

            kaggle.api.authenticate()

            datasets = kaggle.api.dataset_list(page=int(page))

            for dataset in datasets:
                try:
                    result = {
                        "name": dataset.title if hasattr(dataset, 'title') else None,
                        "internalId": dataset.id if hasattr(dataset, 'id') else None,
                        "internalCode": dataset.ref if hasattr(dataset, 'ref') else None,
                        "dataset_tags": ', '.join([tag.name for tag in dataset.tags]),
                        "referenceOwner": 'Kaggle',
                    }

                    ser = InternationalDatasetSerializer(data=result, context={'request': request})
                    if ser.is_valid():
                        ser.validated_data['user'] = request.user
                        ser.save()
                        num_added_records += 1
                    else:
                        print(f"Validation error: {ser.errors}")

                except Exception as e:
                    print(f"Error processing dataset: {e}")

        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response({"response": f"{num_added_records} Records Added"}, status=status.HTTP_201_CREATED)


class TransferKaggleDatasetView(APIView):
    def upload_storage_kaggle(self, dataset_ref):
        """Download Kaggle dataset and upload to S3-compatible storage"""
        if not dataset_ref:
            return {"error": "dataset_ref parameter is required"}, status.HTTP_400_BAD_REQUEST

        try:
            # Load Kaggle credentials
            kaggle_config_path = 'config/kaggle.json'
            if not os.path.exists(kaggle_config_path):
                return {"error": "Kaggle credentials file not found"}, status.HTTP_400_BAD_REQUEST

            with open(kaggle_config_path) as f:
                kaggle_auth = json.load(f)

            os.environ['KAGGLE_USERNAME'] = kaggle_auth.get('username', '')
            os.environ['KAGGLE_KEY'] = kaggle_auth.get('key', '')

            api = kaggle.KaggleApi()
            api.authenticate()

            # Get dataset metadata
            kaggle_temp_path = "./temp/kaggle/"
            os.makedirs(kaggle_temp_path, exist_ok=True)
            kaggle_metadata_path = os.path.join(kaggle_temp_path, f"{dataset_ref.replace('/', '-')}")
            os.makedirs(kaggle_metadata_path, exist_ok=True)

            api.dataset_metadata(dataset_ref, path=kaggle_metadata_path)
            metadata_file = os.path.join(kaggle_metadata_path, 'dataset-metadata.json')

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

            # Generate bucket name
            bucket_name = hashlib.sha256(str(metadata.get("datasetId")).encode()).hexdigest()[:16]

            # Create bucket in S3-compatible storage
            bucket_success, bucket_result = create_s3_bucket(bucket_name)
            if not bucket_success:
                return {
                           "error": "Failed to create S3 bucket",
                           "details": bucket_result
                       }, status.HTTP_500_INTERNAL_SERVER_ERROR

            # Download and upload files
            download_links = []
            total_file_size = 0
            ref_link = f'https://www.kaggle.com/datasets/{dataset_ref}'

            with tempfile.TemporaryDirectory() as temp_dir:
                print(f"Downloading dataset to: {temp_dir}")
                api.dataset_download_files(dataset_ref, path=temp_dir, unzip=True)

                for root, _, files_list in os.walk(temp_dir):
                    for file_name in files_list:
                        file_path = os.path.join(root, file_name)
                        file_size = os.path.getsize(file_path)

                        print(f"Processing file: {file_name} ({file_size} bytes)")

                        # Use the new fallback upload method
                        upload_success, upload_info = upload_with_fallback(
                            file_path,
                            bucket_name,
                            file_name
                        )

                        if upload_success:
                            download_link = {
                                "bucket_name": upload_info["bucket_name"],
                                "object_key": upload_info["object_key"],
                                "size": upload_info["size"],
                                "size_human": upload_info["size_human"]
                            }
                            download_links.append(download_link)
                            total_file_size += upload_info["size"]
                            print(f"Successfully uploaded: {file_name}")
                        else:
                            return {
                                       "error": f"Failed to upload file {file_name}",
                                       "details": upload_info
                                   }, status.HTTP_500_INTERNAL_SERVER_ERROR

                # Cleanup
                if os.path.exists(metadata_file):
                    os.remove(metadata_file)
                if os.path.exists(kaggle_metadata_path):
                    try:
                        os.rmdir(kaggle_metadata_path)
                    except OSError:
                        pass

                # Prepare response
                dataset_details = {
                    "code": bucket_name,
                    "title": metadata.get("title", ""),
                    "datasetId": metadata.get("datasetId"),
                    "description": metadata.get("description", ""),
                    "owner": metadata.get("ownerUser", ""),
                    "totalViews": metadata.get("totalViews", 0),
                    "totalDownloads": metadata.get("totalDownloads", 0),
                    "download_links": download_links,
                    "size": total_file_size,
                    "filesCount": len(download_links),
                    "refLink": ref_link,
                    "keywords": ", ".join(metadata.get("keywords", [])),
                    "licenses": ", ".join([lic.get("name", "") for lic in metadata.get("licenses", [])])
                }

                # Save to databases
                try:
                    self.save_to_databases(dataset_ref, dataset_details, 'Kaggle')
                except Exception as e:
                    print(f"Error saving to database: {str(e)}")

                return dataset_details, status.HTTP_200_OK

        except Exception as e:
            return {
                       "error": "Unexpected error",
                       "details": str(e)
                   }, status.HTTP_500_INTERNAL_SERVER_ERROR

    def save_to_databases(self, dataset_ref, dataset_data, reference_owner='Kaggle'):
        """Save dataset to both InternationalDataset and Dataset tables"""
        try:
            # Update or create InternationalDataset
            international_dataset, created = InternationalDataset.objects.get_or_create(
                internalCode=dataset_ref,
                defaults={
                    'name': dataset_data.get('title', ''),
                    'owner': dataset_data.get('owner', ''),
                    'internalId': str(dataset_data.get('datasetId', '')),
                    'desc': dataset_data.get('description', ''),
                    'license': dataset_data.get('licenses', ''),
                    'dataset_tags': dataset_data.get('keywords', ''),
                    'likes': dataset_data.get('totalViews', 0),
                    'downloads': dataset_data.get('totalDownloads', 0),
                    'size': dataset_data.get('size'),
                    'downloadLink': dataset_data.get('download_links', []),
                    'filesCount': dataset_data.get('filesCount'),
                    'refLink': dataset_data.get('refLink'),
                    'referenceOwner': reference_owner,
                    'dataset_status': 'Download_Completed',
                    'code': dataset_data.get('code', ''),
                    'price': 0.0
                }
            )

            if not created:
                # Update existing record
                international_dataset.name = dataset_data.get('title', '')
                international_dataset.owner = dataset_data.get('owner', '')
                international_dataset.desc = dataset_data.get('description', '')
                international_dataset.license = dataset_data.get('licenses', '')
                international_dataset.dataset_tags = dataset_data.get('keywords', '')
                international_dataset.likes = dataset_data.get('totalViews', 0)
                international_dataset.downloads = dataset_data.get('totalDownloads', 0)
                international_dataset.size = dataset_data.get('size')
                international_dataset.downloadLink = dataset_data.get('download_links', [])
                international_dataset.filesCount = dataset_data.get('filesCount')
                international_dataset.refLink = dataset_data.get('refLink')
                international_dataset.referenceOwner = reference_owner
                international_dataset.dataset_status = 'Download_Completed'
                international_dataset.code = dataset_data.get('code', '')
                international_dataset.price = 0.0
                international_dataset.save()

            # Create or update Dataset entry
            dataset, dataset_created = Dataset.objects.get_or_create(
                name=dataset_data.get('title', ''),
                defaults={
                    'owner': dataset_data.get('owner', ''),
                    'desc': dataset_data.get('description', ''),
                    'license': dataset_data.get('licenses', ''),
                    'size': dataset_data.get('size'),
                    'downloadLink': dataset_data.get('download_links', []),
                    'filesCount': dataset_data.get('filesCount'),
                    'international_dataset': international_dataset
                }
            )

            if not dataset_created:
                # Update existing dataset
                dataset.owner = dataset_data.get('owner', '')
                dataset.desc = dataset_data.get('description', '')
                dataset.license = dataset_data.get('licenses', '')
                dataset.size = dataset_data.get('size')
                dataset.downloadLink = dataset_data.get('download_links', [])
                dataset.filesCount = dataset_data.get('filesCount')
                dataset.international_dataset = international_dataset
                dataset.save()

            return international_dataset, dataset

        except Exception as e:
            print(f"Error saving to databases: {str(e)}")
            raise

    def get(self, request):
        dataset_ref = request.GET.get('dataset_ref')
        if not dataset_ref:
            return Response({"error": "dataset_ref parameter is required"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            # Get dataset details from Kaggle and upload to S3 storage
            dataset_details, status_code = self.upload_storage_kaggle(dataset_ref)
            return Response(dataset_details, status=status_code)

        except Exception as e:
            return Response({
                "error": "Unexpected error",
                "details": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


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

            # Generate bucket name from dataset ID
            bucket_name = hashlib.sha256(str(metadata.get("datasetId")).encode()).hexdigest()[:16]

            # Create bucket in S3-compatible storage
            bucket_success, bucket_result = create_s3_bucket(bucket_name)
            if not bucket_success:
                return Response({
                    "error": "Failed to create S3 bucket",
                    "details": bucket_result
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            # Create temporary directory for download
            with tempfile.TemporaryDirectory() as temp_dir:
                # Download dataset files to temp directory
                api.dataset_download_files(dataset_ref, path=temp_dir, unzip=True)

                # Upload all files to S3 storage using the new fallback method
                file_urls = []
                for root, _, files_list in os.walk(temp_dir):
                    for file_name in files_list:
                        file_path = os.path.join(root, file_name)

                        # Use the new fallback upload method
                        upload_success, upload_result = upload_with_fallback(
                            file_path,
                            bucket_name,
                            file_name
                        )

                        if not upload_success:
                            return Response({
                                "error": f"Failed to upload file {file_name}",
                                "details": upload_result
                            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

                        file_urls.append(upload_result)

            # Prepare response data with S3 storage URLs
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
                "bucket_name": bucket_name,
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

class DownloadSchedulerView(APIView):
    def foo(self):
        print("foo")

    def get(self, request):
        scheduler = BackgroundScheduler()
        scheduler.add_job(self.foo, 'interval', seconds=5)
        scheduler.start()

        return Response({"response": "schedule started"})


##################################################
# DataConvertor
##################################################

import pandas as pd
from fastparquet import write, ParquetFile


class DataConvertorView(APIView):
    def get(self, request):
        dataset_id = request.GET.get('dataset_id')
        response = requests.get(f"https://huggingface.co/api/datasets/{dataset_id}/parquet")
        datasets = response.json()
        print(datasets['default']['test'])
        return Response({"response": datasets})


##################################################
# GenerateMetaData
##################################################

class GenerateMetaData(APIView):
    def post(self, request):
        data = request.data
        result = {
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
        }
        print(result)

        ser = DatasetSerializer(data=result, context={'request': request})
        if ser.is_valid():
            ser.validated_data['user'] = request.user
            instance = ser.save()
            return Response({"response": "Added"}, status=status.HTTP_201_CREATED)
        return Response(ser.errors, status=400)