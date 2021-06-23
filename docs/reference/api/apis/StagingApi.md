# StagingApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**getPhysicalAddress**](StagingApi.md#getPhysicalAddress) | **GET** /repositories/{repository}/branches/{branch}/staging/backing | get a physical address and a return token to write object to underlying storage
[**linkPhysicalAddress**](StagingApi.md#linkPhysicalAddress) | **PUT** /repositories/{repository}/branches/{branch}/staging/backing | associate staging on this physical address with a path


<a name="getPhysicalAddress"></a>
# **getPhysicalAddress**
> StagingLocation getPhysicalAddress(repository, branch, path)

get a physical address and a return token to write object to underlying storage

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **branch** | **String**|  | [default to null]
 **path** | **String**|  | [default to null]

### Return type

[**StagingLocation**](../Models/StagingLocation.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="linkPhysicalAddress"></a>
# **linkPhysicalAddress**
> linkPhysicalAddress(repository, branch, path, StagingMetadata)

associate staging on this physical address with a path

    If the supplied token matches the current staging token, associate the object as the physical address with the supplied path.  Otherwise, if staging has been committed and the token has expired, return a conflict and hint where to place the object to try again.  Caller should copy the object to the new physical address and PUT again with the new staging token.  (No need to back off, this is due to losing the race against a concurrent commit operation.) 

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **branch** | **String**|  | [default to null]
 **path** | **String**|  | [default to null]
 **StagingMetadata** | [**StagingMetadata**](../Models/StagingMetadata.md)|  |

### Return type

null (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

