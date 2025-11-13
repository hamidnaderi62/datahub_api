import os
from pathlib import Path
from datetime import timedelta

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/5.0/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = os.environ.get('SECRET_KEY', 'django-insecure-u=p^-eg)0ui26wiai9w6*frkg9+5o33v(mhx7bmar8fnq&7emu')

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = os.environ.get('DEBUG', 'True').lower() == 'true'

ALLOWED_HOSTS = os.environ.get('ALLOWED_HOSTS', '*').split(',')


# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'rest_framework',
    #'rest_framework.authtoken',
    'drf_spectacular',

    'taggit',
    'dataset.apps.PretermDeliveryConfig'
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'datahub_api.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [BASE_DIR / 'templates']
        ,
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'datahub_api.wsgi.application'


# Database
# https://docs.djangoproject.com/en/5.0/ref/settings/#databases

#DATABASES = {
#    'default': {
#        'ENGINE': 'django.db.backends.sqlite3',
#        'NAME': BASE_DIR / 'db.sqlite3',
#    }
#}


# local
'''
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": "datahub",
        "USER": "postgres",
        "PASSWORD": "123456",
        "HOST": "127.0.0.1",
        "PORT": "5432"
    }
}

'''


# server
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.environ.get('DB_NAME', 'datahub'),
        "USER": os.environ.get('DB_USER', 'green_hub'),
        "PASSWORD": os.environ.get('DB_PASSWORD', 'M@sterhub@2151'),
        "HOST": os.environ.get('DB_HOST', 'datahub_db'),
        "PORT": os.environ.get('DB_PORT', '5432')
    }
}


# Password validation
# https://docs.djangoproject.com/en/5.0/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
# https://docs.djangoproject.com/en/5.0/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/5.0/howto/static-files/

STATIC_URL = 'static/'
MEDIA_URL = 'media/'
MEDIA_ROOT = os.path.join(BASE_DIR, 'media')

# Default primary key field type
# https://docs.djangoproject.com/en/5.0/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

REST_FRAMEWORK = {
    'DEFAULT_AUTHENTICATION_CLASSES': [
        # 'rest_framework.authentication.TokenAuthentication',
        'rest_framework_simplejwt.authentication.JWTAuthentication',
    ],
    # ModelView
    'DEFAULT_PAGINATION_CLASS': 'rest_framework.pagination.LimitOffsetPagination',
    'PAGE_SIZE': 2,

    'DEFAULT_SCHEMA_CLASS': 'drf_spectacular.openapi.AutoSchema',
}

SIMPLE_JWT = {
    "ACCESS_TOKEN_LIFETIME": timedelta(minutes=60),
    "REFRESH_TOKEN_LIFETIME": timedelta(days=1),
}

SPECTACULAR_SETTINGS = {
    'TITLE': 'DataHub API',
    'DESCRIPTION': 'API doc for datahub project',
    'VERSION': '1.0.0',
    'SERVE_INCLUDE_SCHEMA': False,
    'COMPONENT_SPLIT_REQUEST': True
}

STORAGES = {
    "default": {
        "BACKEND": "django.core.files.storage.FileSystemStorage",
        # "BACKEND": "storages.backends.s3boto3.S3Boto3Storage",
    },
    "staticfiles": {
        "BACKEND": "django.contrib.staticfiles.storage.StaticFilesStorage",
    },
}

# Cloud Storage Configuration
CLOUD_STORAGE_CONFIG = {
    'S3_ENDPOINT': os.environ.get('S3_ENDPOINT', 'https://teh-1.s3.poshtiban.com'),
    'ACCESS_KEY': os.environ.get('S3_ACCESS_KEY', '008CA13MP30OHQR2IX5E'),
    'SECRET_KEY': os.environ.get('S3_SECRET_KEY', 'nBLO5dRnuBPVGZLcbuhsWkgsQRHrHKRVe9TLS89X'),
    'REGION': os.environ.get('S3_REGION', 'us-east-1'),
}

# Kaggle Configuration
KAGGLE_USERNAME = os.environ.get('KAGGLE_USERNAME', 'datahub2025')
KAGGLE_KEY = os.environ.get('KAGGLE_KEY', '74d9be44bc9b71717a5ddb97f6e6b939')
KAGGLE_CONFIGURED = bool(KAGGLE_USERNAME and KAGGLE_KEY)

# Log Kaggle configuration status
if KAGGLE_CONFIGURED:
    print("Kaggle credentials configured via environment variables")
else:
    print("Kaggle credentials not configured - Kaggle features will be disabled")