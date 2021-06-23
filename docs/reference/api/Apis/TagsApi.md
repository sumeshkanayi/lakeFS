# TagsApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**createTag**](TagsApi.md#createTag) | **POST** /repositories/{repository}/tags | create tag
[**deleteTag**](TagsApi.md#deleteTag) | **DELETE** /repositories/{repository}/tags/{tag} | delete tag
[**getTag**](TagsApi.md#getTag) | **GET** /repositories/{repository}/tags/{tag} | get tag
[**listTags**](TagsApi.md#listTags) | **GET** /repositories/{repository}/tags | list tags


<a name="createTag"></a>
# **createTag**
> Ref createTag(repository, TagCreation)

create tag

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **TagCreation** | [**TagCreation**](../Models/TagCreation.md)|  |

### Return type

[**Ref**](../Models/Ref.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="deleteTag"></a>
# **deleteTag**
> deleteTag(repository, tag)

delete tag

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **tag** | **String**|  | [default to null]

### Return type

null (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getTag"></a>
# **getTag**
> Ref getTag(repository, tag)

get tag

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **tag** | **String**|  | [default to null]

### Return type

[**Ref**](../Models/Ref.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="listTags"></a>
# **listTags**
> RefList listTags(repository, after, amount)

list tags

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **after** | **String**| return items after this value | [optional] [default to null]
 **amount** | **Integer**| how many items to return | [optional] [default to 100]

### Return type

[**RefList**](../Models/RefList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

