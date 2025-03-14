from django.shortcuts import render
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework.views import APIView
import requests
from rest_framework import status
from rest_framework.authentication import TokenAuthentication
from rest_framework.permissions import IsAuthenticated, IsAdminUser
from django.contrib.auth.models import User
from .serializers import UserSerializer, UserDetailSerializer, UserFullDetailSerializer, PretermSerializer, CommentSerializer
from .models import Preterm, Comment
from .permissions import BlocklistPermission, IsOwnerOrReadOnly

@api_view(['GET', 'POST'])
def hello_world(request):
    data = request.data
    return Response({"message": f"{data['fname']}  {data.get('lname')}"})


class HelloWorld(APIView):
    def get(self, request):
        return Response({"message": "Hi get"})
    def post(self, request):
        return Response({"message": "Hi post"})


class CryptoPriceListView(APIView):
    def get(self, request):
        coin = request.GET.get('coin')
        response = requests.get(f"https://api.binance.com/api/v3/ticker/price?symbol={coin.upper()}")
        data = response.json()
        result = {
            "symbol": data['symbol'],
            "price": data['price']
        }
        return Response(data=result)

class UsersListView(APIView):
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


class PretermsListView(APIView):
    def get(self, request):
        queryset = Preterm.objects.all()
        ser = PretermSerializer(instance=queryset, many=True)
        return Response(data=ser.data)


class PretermDetailView(APIView):
    def get(self, request, pk):
        instance = Preterm.objects.get(id=pk)
        ser = PretermSerializer(instance=instance)
        return Response(data=ser.data)


class AddPretermView(APIView):
    permission_classes = [IsAuthenticated, BlocklistPermission]
    # permission_classes = [IsAdminUser]
    def post(self, request):
        ser = PretermSerializer(data=request.data, context={'request': request})
        if ser.is_valid():
            ser.validated_data['user'] = request.user
            instance = ser.save()
            return Response({"response": "Added"}, status=status.HTTP_201_CREATED)
        return Response(ser.errors, status=400)


class AddPretermView1(APIView):
    def post(self, request):
        ser = PretermSerializer(data=request.data)
        if ser.is_valid():
            if request.user.is_authenticated:
                ser.validated_data['user'] = request.user
                instance = ser.save()
                # instance.preterm_delivery = 1
                # instance.save()
                return Response({"response": "Added"}, status=status.HTTP_201_CREATED)
            return Response(ser.errors, status=status.HTTP_401_UNAUTHORIZED)
        return Response(ser.errors, status=400)


class UpdatePretermView(APIView):
    permission_classes = [IsAuthenticated, IsOwnerOrReadOnly]
    def put(self, request, pk):
        instance = Preterm.objects.get(id=pk)
        self.check_object_permissions(request, instance)
        ser = PretermSerializer(instance=instance, data=request.data, partial=True)
        if ser.is_valid():
            instance = ser.save()
            return Response({"response": "Updated"})
        return Response(ser.errors)


class DeletePretermView(APIView):
    def delete(self, request, pk):
        instance = Preterm.objects.get(id=pk)
        instance.delete()
        return Response({"response": "Deleted"})


class CheckToken(APIView):
    # authentication_classes = [TokenAuthentication]
    def get(self, request):
        user = request.user
        return Response({"user": user.username}, status=status.HTTP_200_OK)


class CommentsListView(APIView):
    def get(self, request, pk):
        comments = Preterm.objects.get(id=pk).comments.all()
        serializer = CommentSerializer(instance=comments, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)
