from django.contrib.auth.models import User
from django.db import models



class BlockUser(models.Model):
    username = models.CharField(max_length=100, unique=True)

    def __str__(self):
        return self.username


class InternationalDataset(models.Model):
    CREATE_TYPE = (
        ('Create', 'Create'),
        ('Transfer', 'Transfer')
    )

    DATA_TYPE = (
        ('Text', 'Text'),
        ('Image', 'Image'),
        ('Audio', 'Audio'),
        ('Video', 'Video'),
        ('GeoData', 'GeoData')
    )

    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='international_datasets', blank=True, null=True)
    code = models.CharField(max_length=100, blank=True, null=True)
    name = models.CharField(max_length=1000, blank=True, null=True)
    owner = models.CharField(max_length=1000, blank=True, null=True)
    internalId = models.CharField(max_length=300, blank=True, null=True)
    internalCode = models.CharField(max_length=300, blank=True, null=True)
    recordsNum = models.CharField(max_length=10, blank=True, null=True)
    size = models.CharField(max_length=30, blank=True, null=True)
    format = models.CharField(max_length=100, blank=True, null=True)
    language = models.CharField(max_length=30, blank=True, null=True)
    desc = models.TextField(blank=True, null=True)
    license = models.CharField(max_length=100, blank=True, null=True)
    tasks = models.CharField(max_length=1000, blank=True, null=True)
    datasetDate = models.DateTimeField(blank=True, null=True)
    columnDataType = models.JSONField(blank=True, null=True)
    sourceJson = models.JSONField(blank=True, null=True)
    image = models.ImageField(upload_to='images', blank=True, null=True)
    downloadLink = models.JSONField(blank=True, null=True)
    price = models.FloatField(blank=True, null=True)
    downloadCount = models.IntegerField(blank=True, null=True)
    createType = models.CharField(max_length=50, choices=CREATE_TYPE, default='Transfer', blank=True, null=True)
    referenceOwner = models.CharField(max_length=500, blank=True, null=True)
    datasetRate = models.FloatField(blank=True, null=True)
    dataType = models.CharField(max_length=50, choices=DATA_TYPE, default='Text', blank=True, null=True)
    dataset_tags = models.TextField(blank=True, null=True)
    likes = models.IntegerField(blank=True, null=True)
    downloads = models.IntegerField(blank=True, null=True)
    created = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.name[:50]


class Dataset(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='datasets', blank=True, null=True)
    code = models.CharField(max_length=100, blank=True, null=True)
    name = models.CharField(max_length=1000, blank=True, null=True)
    owner = models.CharField(max_length=1000, blank=True, null=True)
    internalId = models.CharField(max_length=300, blank=True, null=True)
    internalCode = models.CharField(max_length=300, blank=True, null=True)
    recordsNum = models.CharField(max_length=10, blank=True, null=True)
    size = models.CharField(max_length=30, blank=True, null=True)
    format = models.CharField(max_length=30, blank=True, null=True)
    language = models.CharField(max_length=30, blank=True, null=True)
    desc = models.TextField(blank=True, null=True)
    license = models.CharField(max_length=100, blank=True, null=True)
    tasks = models.CharField(max_length=1000, blank=True, null=True)
    datasetDate = models.DateTimeField(blank=True, null=True)
    columnDataType = models.JSONField(blank=True, null=True)
    sourceJson = models.JSONField(blank=True, null=True)
    image = models.ImageField(upload_to='images', blank=True, null=True)

    created = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.name[:50]


class Comment(models.Model):
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE, related_name="comments")
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name="comments")
    text = models.TextField()
    Date = models.DateField(auto_now_add=True)

    def __str__(self):
        return self.text[:30]

