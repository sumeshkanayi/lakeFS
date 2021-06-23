# ConfigApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**getLakeFSVersion**](ConfigApi.md#getLakeFSVersion) | **GET** /config/version | 
[**getStorageConfig**](ConfigApi.md#getStorageConfig) | **GET** /config/storage | 
[**setup**](ConfigApi.md#setup) | **POST** /setup_lakefs | setup lakeFS and create a first user


<a name="getLakeFSVersion"></a>
# **getLakeFSVersion**
> VersionConfig getLakeFSVersion()



    get version of lakeFS server

### Parameters
This endpoint does not need any parameter.

### Return type

[**VersionConfig**](../Models/VersionConfig.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getStorageConfig"></a>
# **getStorageConfig**
> StorageConfig getStorageConfig()



    retrieve lakeFS storage configuration

### Parameters
This endpoint does not need any parameter.

### Return type

[**StorageConfig**](../Models/StorageConfig.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="setup"></a>
# **setup**
> CredentialsWithSecret setup(Setup)

setup lakeFS and create a first user

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **Setup** | [**Setup**](../Models/Setup.md)|  |

### Return type

[**CredentialsWithSecret**](../Models/CredentialsWithSecret.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

