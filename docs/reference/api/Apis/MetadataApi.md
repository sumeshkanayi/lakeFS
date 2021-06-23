# MetadataApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**createSymlinkFile**](MetadataApi.md#createSymlinkFile) | **POST** /repositories/{repository}/refs/{branch}/symlink | creates symlink files corresponding to the given directory
[**getMetaRange**](MetadataApi.md#getMetaRange) | **GET** /repositories/{repository}/metadata/meta_range/{meta_range} | return URI to a meta-range file
[**getRange**](MetadataApi.md#getRange) | **GET** /repositories/{repository}/metadata/range/{range} | return URI to a range file


<a name="createSymlinkFile"></a>
# **createSymlinkFile**
> StorageURI createSymlinkFile(repository, branch, location)

creates symlink files corresponding to the given directory

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **branch** | **String**|  | [default to null]
 **location** | **String**| path to the table data | [optional] [default to null]

### Return type

[**StorageURI**](../Models/StorageURI.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getMetaRange"></a>
# **getMetaRange**
> StorageURI getMetaRange(repository, meta\_range)

return URI to a meta-range file

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **meta\_range** | **String**|  | [default to null]

### Return type

[**StorageURI**](../Models/StorageURI.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getRange"></a>
# **getRange**
> StorageURI getRange(repository, range)

return URI to a range file

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **range** | **String**|  | [default to null]

### Return type

[**StorageURI**](../Models/StorageURI.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

