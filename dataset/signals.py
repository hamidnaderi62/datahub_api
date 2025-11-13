from django.db.models.signals import post_save
from django.dispatch import receiver
from .models import InternationalDataset, Dataset


@receiver(post_save, sender=InternationalDataset)
def update_or_create_dataset_after_download(sender, instance, **kwargs):
    """
    This signal is triggered when an InternationalDataset object is saved.
    If the dataset_status is 'Download_Completed', it will either update an existing Dataset
    or create a new one if it does not exist.
    """
    if instance.dataset_status == "Download_Completed":
        dataset, created = Dataset.objects.update_or_create(
            name=instance.name,  # Match dataset by name
            defaults={
                "user": instance.user,
                "code": instance.code,
                "name": instance.name,
                "owner": instance.owner,
                "internalId": instance.internalId,
                "internalCode": instance.internalCode,
                "recordsNum": instance.recordsNum,
                "size": instance.size,
                "filesCount": instance.filesCount,
                "refLink": instance.refLink,
                "format": instance.format,
                "language": instance.language,
                "desc": instance.desc,
                "license": instance.license,
                "tasks": instance.tasks,
                "datasetDate": instance.datasetDate,
                "columnDataType": instance.columnDataType,
                "image": instance.image,
                "downloadLink": instance.downloadLink,
                "referenceOwner": instance.referenceOwner,
                "createType": instance.createType,
                "dataType": instance.dataType,
                "dataset_tags": instance.dataset_tags,
                "tags": instance.dataset_tags,
                "created": instance.created,
                "price": instance.price
            }
        )

        if created:
            print(f"Dataset '{instance.name}' was created from InternationalDataset.")
        else:
            print(f"Dataset '{instance.name}' was updated from InternationalDataset.")