from django.contrib.auth.models import User
from django.db import models



class BlockUser(models.Model):
    username = models.CharField(max_length=100, unique=True)

    def __str__(self):
        return self.username


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

