# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.http import HttpResponse
from django.contrib.auth.models import User

# Create your views here.
from rest_framework.permissions import AllowAny
from rest_framework.decorators import api_view, action, authentication_classes, permission_classes
from rest_framework.viewsets import ModelViewSet
from rest_framework.response import Response
from rest_framework import status

from restapi.serializers import Expenses, Groups, UserSerializer, Category, CategorySerializer, GroupSerializer, ExpensesSerializer, UserExpense
from restapi.custom_exception import UnauthorizedUserException
from restapi.utils import calculate_time, multi_threaded_reader, sort_by_time_stamp, transform, aggregate, response_format

import logging

logger = logging.getLogger(__name__)


def index() -> HttpResponse:
    return HttpResponse("Hello, world. You're at Rest.")

@api_view(['POST'])
@calculate_time(logger)
def logout(request) -> Response:
    """Delete user authentication token"""
    request.user.auth_token.delete()
    logger.info(f"{request.user} has logged out successfully!")
    return Response(status=status.HTTP_204_NO_CONTENT)


@api_view(['GET'])
@calculate_time(logger)
def balance(request) -> Response:
    """Calculates and return the final balance of the requested user"""
    user = request.user
    logger.info(f"Final balance of {user} requested")
    expenses = Expenses.objects.filter(users__in=user.expenses.all())
    final_balance = {}
    for expense in expenses:
        expense_balances = normalize(expense)
        for eb in expense_balances:
            from_user = eb['from_user']
            to_user = eb['to_user']
            if from_user == user.id:
                final_balance[to_user] = final_balance.get(to_user, 0) - eb['amount']
            if to_user == user.id:
                final_balance[from_user] = final_balance.get(from_user, 0) + eb['amount']
    final_balance = {k: v for k, v in final_balance.items() if v != 0}

    response = [{"user": k, "amount": int(v)} for k, v in final_balance.items()]
    logger.info(f"Final balance of {user}: {response}")
    return Response(response, status=status.HTTP_200_OK)

def normalize_dues(dues):
    """Normalize the dues and retunrn the balances"""
    dues = [(k, v) for k, v in sorted(dues.items(), key=lambda item: item[1])]
    start = 0
    end = len(dues) - 1
    balances = []
    while start < end:
        amount = min(abs(dues[start][1]), abs(dues[end][1]))
        user_balance = {"from_user": dues[start][0].id, "to_user": dues[end][0].id, "amount": amount}
        balances.append(user_balance)
        dues[start] = (dues[start][0], dues[start][1] + amount)
        dues[end] = (dues[end][0], dues[end][1] - amount)
        if dues[start][1] == 0:
            start += 1
        else:
            end -= 1
    return balances

@calculate_time(logger)
def normalize(expenses) -> list:
    """Normalize the expenses and retunrn the balances"""
    logging.info(f"Normalizing requested for {expenses}")
    user_balances = expenses.users.all()
    dues = {}
    for user_balance in user_balances:
        dues[user_balance.user] = dues.get(user_balance.user, 0) + user_balance.amount_lent \
                                  - user_balance.amount_owed
    balances = normalize_dues(dues)
    logging.info(f"The normalized balances: {balances}")
    return balances


class UserViewSet(ModelViewSet):
    """User View Set"""
    queryset = User.objects.all()
    serializer_class = UserSerializer
    permission_classes = (AllowAny,)


class CategoryViewSet(ModelViewSet):
    """Category View Set"""
    queryset = Category.objects.all()
    serializer_class = CategorySerializer
    http_method_names = ['get', 'post']


class GroupViewSet(ModelViewSet):
    """Group View Set"""
    queryset = Groups.objects.all()
    serializer_class = GroupSerializer

    def get_queryset(self):
        """Return the groupset of the user"""
        user = self.request.user
        groups = user.members.all()
        if self.request.query_params.get('q', None) is not None:
            groups = groups.filter(name__icontains=self.request.query_params.get('q', None))
        return groups

    def create(self) -> Response:
        """Create a group and adds the user to it"""
        user = self.request.user
        data = self.request.data
        group = Groups(**data)
        group.save()
        group.members.add(user)
        serializer = self.get_serializer(group)
        return Response(serializer.data, status=status.HTTP_201_CREATED)

    @action(methods=['put'], detail=True)
    def members(self, request, pk=None) -> Response:
        """Add member to a group"""
        group = Groups.objects.get(id=pk)
        if group not in self.get_queryset():
            raise UnauthorizedUserException()
        body = request.data
        if body.get('add', None) is not None and body['add'].get('user_ids', None) is not None:
            added_ids = body['add']['user_ids']
            for user_id in added_ids:
                group.members.add(user_id)
        if body.get('remove', None) is not None and body['remove'].get('user_ids', None) is not None:
            removed_ids = body['remove']['user_ids']
            for user_id in removed_ids:
                group.members.remove(user_id)
        group.save()
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(methods=['get'], detail=True)
    def expenses(self, pk=None) -> Response:
        """Return expenses for a group"""
        group = Groups.objects.get(id=pk)
        if group not in self.get_queryset():
            raise UnauthorizedUserException()
        expenses = group.expenses_set
        serializer = ExpensesSerializer(expenses, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    @action(methods=['get'], detail=True)
    def balances(self, pk=None) -> Response:
        """Return balance for a group"""
        group = Groups.objects.get(id=pk)
        if group not in self.get_queryset():
            raise UnauthorizedUserException()
        expenses = Expenses.objects.filter(group=group)
        dues = {}
        for expense in expenses:
            user_balances = UserExpense.objects.filter(expense=expense)
            for user_balance in user_balances:
                dues[user_balance.user] = dues.get(user_balance.user, 0) + user_balance.amount_lent \
                                          - user_balance.amount_owed
        balances = normalize_dues(dues)

        return Response(balances, status=status.HTTP_200_OK)


class ExpensesViewSet(ModelViewSet):
    queryset = Expenses.objects.all()
    serializer_class = ExpensesSerializer

    def get_queryset(self):
        """Return the expenses queryset"""
        user = self.request.user
        if self.request.query_params.get('q', None) is not None:
            expenses = Expenses.objects.filter(users__in=user.expenses.all())\
                .filter(description__icontains=self.request.query_params.get('q', None))
        else:
            expenses = Expenses.objects.filter(users__in=user.expenses.all())
        return expenses

@api_view(['post'])
@authentication_classes([])
@permission_classes([])
def log_processor(request) -> Response:
    """Process log files"""
    data = request.data
    num_threads = data['parallelFileProcessingCount']
    log_files = data['logFiles']
    if num_threads <= 0 or num_threads > 30:
        return Response({"status": "failure", "reason": "Parallel Processing Count out of expected bounds"},
                        status=status.HTTP_400_BAD_REQUEST)
    if len(log_files) == 0:
        return Response({"status": "failure", "reason": "No log files provided in request"},
                        status=status.HTTP_400_BAD_REQUEST)
    logs = multi_threaded_reader(urls=data['logFiles'], num_threads=data['parallelFileProcessingCount'])
    sorted_logs = sort_by_time_stamp(logs)
    cleaned = transform(sorted_logs)
    data = aggregate(cleaned)
    response = response_format(data)
    return Response({"response":response}, status=status.HTTP_200_OK)
