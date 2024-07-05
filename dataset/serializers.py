from rest_framework import serializers
from django.utils.timezone import now
from persiantools.jdatetime import JalaliDate
from .models import Dataset, Comment, User


class UserSerializer(serializers.Serializer):
    username = serializers.CharField(max_length=100)
    email = serializers.EmailField(max_length=100)
    password = serializers.CharField(write_only=True)

    def create(self, validated_data):
        user = User.objects.create_user(
            username=validated_data['username'],
            password=validated_data['password'],
        )
        return user

    class Meta:
        model = User
        fields = ("id", "username", "password", )


class UserDetailSerializer(serializers.ModelSerializer):
    datasets = serializers.PrimaryKeyRelatedField(read_only=True, many=True)
    class Meta:
        model = User
        fields = "__all__"

'''
class PretermSerializer(serializers.Serializer):
    id = serializers.IntegerField(required=False)
    diabetes_in_pregnancy = serializers.CharField(max_length=10)
    newborn_under_2500g = serializers.CharField(max_length=10)
    abnormalBaby = serializers.CharField(max_length=10)
    multiple_births = serializers.CharField(max_length=10)
    preterm_delivery = serializers.CharField(max_length=10, required=False)

    def create(self, validated_data):
        return Preterm.objects.create(**validated_data)
        # return Preterm.objects.create(diabetes_in_pregnancy=validated_data['diabetes_in_pregnancy'])
'''


class DatasetSerializer(serializers.ModelSerializer):
    code = serializers.CharField(read_only=True)
    # days_ago = serializers.SerializerMethodField()
    # persian_date = serializers.SerializerMethodField()
    comments = serializers.SerializerMethodField()
    user = serializers.SlugRelatedField(read_only=True, slug_field="username")
    image = serializers.SerializerMethodField()
    class Meta:
        model = Dataset
        fields = "__all__"
        # fields = ("id", "preterm_delivery", "days_ago", "persian_date", "user", "comments")
        # read_only_fields = ['preterm_delivery']
        # exclude = ("preterm_delivery",)

    # Field based validation
    # def validate_diabetes_in_pregnancy(self, value):
    #     if value != "1" and value != "0":
    #         raise serializers.ValidationError("Your input is not valid")
    #     return value

    # Object based validation
#    def validate(self, attrs):
#        if attrs['name'] == "1" and attrs['owner'] == "0":
#            raise serializers.ValidationError("Your input is not valid")
#        return attrs

    def create(self, validated_data):
        request = self.context.get('request')
        validated_data['user'] = request.user
        return Dataset.objects.create(**validated_data)

    def get_days_ago(self, obj):
        return (now().date() - obj.created.date()).days

    def get_persian_date(self, obj):
        persian_date = JalaliDate(obj.created.date(), locale="fa")
        return persian_date.strftime('%c')

    def get_comments(self, obj):
        serializer = CommentSerializer(instance=obj.comments.all(), many=True)
        return serializer.data

    def get_image(self, obj):
        request = self.context.get('request')
        if obj.image:
            image_url = obj.image.url
            return request.build_absolute_uri(image_url)
        return None


class CommentSerializer(serializers.ModelSerializer):
    class Meta:
        model = Comment
        fields = "__all__"


class UserFullDetailSerializer(serializers.ModelSerializer):
    datasets = DatasetSerializer(many=True)
    class Meta:
        model = User
        fields = "__all__"