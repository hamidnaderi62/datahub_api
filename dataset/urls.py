from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views

# Create a router and register our viewsets with it.
router = DefaultRouter()
# router.register(r'datasets', views.DatasetViewSet)

urlpatterns = [
    path('', include(router.urls)),

    # Kaggle endpoints
    path('TransferKaggleDataset', views.TransferKaggleDatasetView.as_view(), name='transfer_kaggle_dataset'),
    path('KaggleDatasetDetail', views.KaggleDatasetDetailView.as_view(), name='kaggle_dataset_detail'),
    path('KaggleDatasetsList', views.KaggleDatasetsListView.as_view(), name='kaggle_datasets_list'),
    path('BulkImportKaggle', views.BulkImportKaggleView.as_view(), name='bulk_import_kaggle'),
    path('UploadStorageKaggle', views.UploadStorageKaggleView.as_view(), name='upload_storage_kaggle'),

    # HuggingFace endpoints
    path('TransferHuggingfaceDataset', views.TransferHuggingfaceDatasetView.as_view(),
         name='transfer_huggingface_dataset'),
    path('HuggingfaceDatasetDetail', views.HuggingfaceDatasetDetailView.as_view(), name='huggingface_dataset_detail'),
    path('HuggingfaceDatasetsList', views.HuggingfaceDatasetsListView.as_view(), name='huggingface_datasets_list'),
    path('BulkImportHuggingface', views.BulkImportHuggingfaceView.as_view(), name='bulk_import_huggingface'),
    path('ImportHuggingface', views.ImportHuggingfaceView.as_view(), name='import_huggingface'),
    path('HuggingfaceParquetFiles', views.HuggingfaceParquetFilesView.as_view(), name='huggingface_parquet_files'),

    # Other data sources
    path('PaperWithCodeDatasetsList', views.PaperWithCodeDatasetsListView.as_view(),
         name='paperwithcode_datasets_list'),

    # User management
    path('UsersList', views.UsersListView.as_view(), name='users_list'),
    path('UsersListDetail', views.UsersListDetailView.as_view(), name='users_list_detail'),
    path('UsersListFullDetail', views.UsersListFullDetailView.as_view(), name='users_list_full_detail'),
    path('AddUser', views.AddUserView.as_view(), name='add_user'),
    path('CheckToken', views.CheckToken.as_view(), name='check_token'),

    # Dataset management
    path('DatasetsList', views.DatasetsListView.as_view(), name='datasets_list'),
    path('DatasetDetail/<int:pk>', views.DatasetDetailView.as_view(), name='dataset_detail'),
    path('AddDataset', views.AddDatasetView.as_view(), name='add_dataset'),
    path('UpdateDataset/<int:pk>', views.UpdateDatasetView.as_view(), name='update_dataset'),
    path('DeleteDataset/<int:pk>', views.DeleteDatasetView.as_view(), name='delete_dataset'),
    path('SearchDataset', views.SearchDatasetView.as_view(), name='search_dataset'),
    path('DatasetSample', views.DatasetSampleView.as_view(), name='dataset_sample'),

    # Comments
    path('CommentsList/<int:pk>', views.CommentsListView.as_view(), name='comments_list'),

    # Utilities
    path('DownloadScheduler', views.DownloadSchedulerView.as_view(), name='download_scheduler'),
    path('DataConvertor', views.DataConvertorView.as_view(), name='data_convertor'),
    path('GenerateMetaData', views.GenerateMetaData.as_view(), name='generate_metadata'),
]