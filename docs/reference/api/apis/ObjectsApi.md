# ObjectsApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**deleteObject**](ObjectsApi.md#deleteObject) | **DELETE** /repositories/{repository}/branches/{branch}/objects | delete object
[**getObject**](ObjectsApi.md#getObject) | **GET** /repositories/{repository}/refs/{ref}/objects | get object content
[**getUnderlyingProperties**](ObjectsApi.md#getUnderlyingProperties) | **GET** /repositories/{repository}/refs/{ref}/objects/underlyingProperties | get object properties on underlying storage
[**listObjects**](ObjectsApi.md#listObjects) | **GET** /repositories/{repository}/refs/{ref}/objects/ls | list objects under a given prefix
[**stageObject**](ObjectsApi.md#stageObject) | **PUT** /repositories/{repository}/branches/{branch}/objects | stage an object\&quot;s metadata for the given branch
[**statObject**](ObjectsApi.md#statObject) | **GET** /repositories/{repository}/refs/{ref}/objects/stat | get object metadata
[**uploadObject**](ObjectsApi.md#uploadObject) | **POST** /repositories/{repository}/branches/{branch}/objects | 


<a name="deleteObject"></a>
# **deleteObject**
> deleteObject(repository, branch, path)

delete object

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **branch** | **String**|  | [default to null]
 **path** | **String**|  | [default to null]

### Return type

null (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getObject"></a>
# **getObject**
> File getObject(repository, ref, path)

get object content

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **ref** | **String**| a reference (could be either a branch or a commit ID) | [default to null]
 **path** | **String**|  | [default to null]

### Return type

[**File**](../Models/file.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/octet-stream, application/json

<a name="getUnderlyingProperties"></a>
# **getUnderlyingProperties**
> UnderlyingObjectProperties getUnderlyingProperties(repository, ref, path)

get object properties on underlying storage

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **ref** | **String**| a reference (could be either a branch or a commit ID) | [default to null]
 **path** | **String**|  | [default to null]

### Return type

[**UnderlyingObjectProperties**](../Models/UnderlyingObjectProperties.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="listObjects"></a>
# **listObjects**
> ObjectStatsList listObjects(repository, ref, after, amount, delimiter, prefix)

list objects under a given prefix

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **ref** | **String**| a reference (could be either a branch or a commit ID) | [default to null]
 **after** | **String**| return items after this value | [optional] [default to null]
 **amount** | **Integer**| how many items to return | [optional] [default to 100]
 **delimiter** | **String**| delimiter used to group common prefixes by | [optional] [default to null]
 **prefix** | **String**| return items prefixed with this value | [optional] [default to null]

### Return type

[**ObjectStatsList**](../Models/ObjectStatsList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="stageObject"></a>
# **stageObject**
> ObjectStats stageObject(repository, branch, path, ObjectStageCreation)

stage an object\&quot;s metadata for the given branch

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **branch** | **String**|  | [default to null]
 **path** | **String**|  | [default to null]
 **ObjectStageCreation** | [**ObjectStageCreation**](../Models/ObjectStageCreation.md)|  |

### Return type

[**ObjectStats**](../Models/ObjectStats.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="statObject"></a>
# **statObject**
> ObjectStats statObject(repository, ref, path)

get object metadata

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **ref** | **String**| a reference (could be either a branch or a commit ID) | [default to null]
 **path** | **String**|  | [default to null]

### Return type

[**ObjectStats**](../Models/ObjectStats.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="uploadObject"></a>
# **uploadObject**
> ObjectStats uploadObject(repository, branch, path, storageClass, If-None-Match, content)



### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **branch** | **String**|  | [default to null]
 **path** | **String**|  | [default to null]
 **storageClass** | **String**|  | [optional] [default to null]
 **If-None-Match** | **String**| Currently supports only \&quot;*\&quot; to allow uploading an object only if one doesn&#39;t exist yet | [optional] [default to null]
 **content** | **File**| Object content to upload | [optional] [default to null]

### Return type

[**ObjectStats**](../Models/ObjectStats.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: multipart/form-data
- **Accept**: application/json

