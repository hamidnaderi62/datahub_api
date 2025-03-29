from django.apps import AppConfig


class PretermDeliveryConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'dataset'
    def ready(self):
        import dataset.signals  # Import signals to ensure they are loaded