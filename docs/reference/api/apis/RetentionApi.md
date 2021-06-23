# RetentionApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**getGarbageCollectionRules**](RetentionApi.md#getGarbageCollectionRules) | **GET** /repositories/{repository}/gc/rules | 
[**prepareGarbageCollectionCommits**](RetentionApi.md#prepareGarbageCollectionCommits) | **POST** /repositories/{repository}/gc/prepare_commits | save lists of active and expired commits for garbage collection
[**set garbage collection rules**](RetentionApi.md#set garbage collection rules) | **POST** /repositories/{repository}/gc/rules | 


<a name="getGarbageCollectionRules"></a>
# **getGarbageCollectionRules**
> GarbageCollectionRules getGarbageCollectionRules(repository)



### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]

### Return type

[**GarbageCollectionRules**](../Models/GarbageCollectionRules.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="prepareGarbageCollectionCommits"></a>
# **prepareGarbageCollectionCommits**
> GarbageCollectionPrepareResponse prepareGarbageCollectionCommits(repository, GarbageCollectionPrepareRequest)

save lists of active and expired commits for garbage collection

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **GarbageCollectionPrepareRequest** | [**GarbageCollectionPrepareRequest**](../Models/GarbageCollectionPrepareRequest.md)|  | [optional]

### Return type

[**GarbageCollectionPrepareResponse**](../Models/GarbageCollectionPrepareResponse.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="set garbage collection rules"></a>
# **set garbage collection rules**
> set garbage collection rules(repository, GarbageCollectionRules)



### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **GarbageCollectionRules** | [**GarbageCollectionRules**](../Models/GarbageCollectionRules.md)|  |

### Return type

null (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

