# RepositoriesApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**createRepository**](RepositoriesApi.md#createRepository) | **POST** /repositories | create repository
[**deleteRepository**](RepositoriesApi.md#deleteRepository) | **DELETE** /repositories/{repository} | delete repository
[**getRepository**](RepositoriesApi.md#getRepository) | **GET** /repositories/{repository} | get repository
[**listRepositories**](RepositoriesApi.md#listRepositories) | **GET** /repositories | list repositories


<a name="createRepository"></a>
# **createRepository**
> Repository createRepository(RepositoryCreation, bare)

create repository

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **RepositoryCreation** | [**RepositoryCreation**](../Models/RepositoryCreation.md)|  |
 **bare** | **Boolean**| If true, create a bare repository with no initial commit and branch | [optional] [default to false]

### Return type

[**Repository**](../Models/Repository.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="deleteRepository"></a>
# **deleteRepository**
> deleteRepository(repository)

delete repository

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]

### Return type

null (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getRepository"></a>
# **getRepository**
> Repository getRepository(repository)

get repository

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]

### Return type

[**Repository**](../Models/Repository.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="listRepositories"></a>
# **listRepositories**
> RepositoryList listRepositories(prefix, after, amount)

list repositories

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **String**| return items prefixed with this value | [optional] [default to null]
 **after** | **String**| return items after this value | [optional] [default to null]
 **amount** | **Integer**| how many items to return | [optional] [default to 100]

### Return type

[**RepositoryList**](../Models/RepositoryList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

