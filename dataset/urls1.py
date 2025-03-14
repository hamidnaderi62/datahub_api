from django.urls import path
from . import views
from rest_framework.authtoken import views as token_views
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView

#ViewSet
from rest_framework.routers import DefaultRouter

urlpatterns = [
    path('dataset', views.hello_world),
    path('dataset/cbv', views.HelloWorld.as_view()),
    path('crypto', views.CryptoPriceListView.as_view()),
    path('users', views.UsersListView.as_view()),
    path('users_detail', views.UsersListDetailView.as_view()),
    path('users_fulldetail', views.UsersListFullDetailView.as_view()),
    path('users/add', views.AddUserView.as_view()),

    path('datasets', views.DatasetsListView.as_view()),
    path('datasets/<int:pk>', views.DatasetDetailView.as_view()),
    path('datasets/sample', views.DatasetSampleView.as_view()),
    path('datasets/add', views.AddDatasetView.as_view()),
    path('datasets/update/<int:pk>', views.UpdateDatasetView.as_view()),
    path('datasets/delete/<int:pk>', views.DeleteDatasetView.as_view()),
    path('datasets/search', views.SearchDatasetView.as_view()),
    path('datasets/checktoken', views.CheckToken.as_view()),

    # DownloadScheduler
    path('DatasetDownloadScheduler', views.DownloadSchedulerView.as_view()),

    # DataConvertor
    path('convertToParquet', views.DataConvertorView.as_view()),

    # GenerateMetaData
    path('GenerateMetaData', views.GenerateMetaData.as_view()),

    # HuggingFace
    path('HuggingfaceDatasetsList', views.HuggingfaceDatasetsListView.as_view()),
    path('HuggingfaceDatasetDetail', views.HuggingfaceDatasetDetailView.as_view()),
    path('ImportHuggingface', views.ImportHuggingfaceView.as_view()),
    path('BulkImportHuggingface', views.BulkImportHuggingfaceView.as_view()),

    # Kaggle
    path('KaggleDatasetsList', views.KaggleDatasetsListView.as_view()),

    # TokenAuthentication
    # path('preterms/login', token_views.obtain_auth_token),

    # JWTAuthentication
    path('login', TokenObtainPairView.as_view(), name='token_obtain_pair'),
    path('refresh', TokenRefreshView.as_view(), name='token_refresh'),

    path('comments/<int:pk>', views.CommentsListView.as_view()),
]


router = DefaultRouter()
router.register(r'datasets/viewset', views.DatasetViewSet, basename='datasets')
urlpatterns += router.urls