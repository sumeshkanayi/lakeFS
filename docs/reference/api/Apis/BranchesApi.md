# BranchesApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**createBranch**](BranchesApi.md#createBranch) | **POST** /repositories/{repository}/branches | create branch
[**deleteBranch**](BranchesApi.md#deleteBranch) | **DELETE** /repositories/{repository}/branches/{branch} | delete branch
[**diffBranch**](BranchesApi.md#diffBranch) | **GET** /repositories/{repository}/branches/{branch}/diff | diff branch
[**getBranch**](BranchesApi.md#getBranch) | **GET** /repositories/{repository}/branches/{branch} | get branch
[**listBranches**](BranchesApi.md#listBranches) | **GET** /repositories/{repository}/branches | list branches
[**resetBranch**](BranchesApi.md#resetBranch) | **PUT** /repositories/{repository}/branches/{branch} | reset branch
[**revertBranch**](BranchesApi.md#revertBranch) | **POST** /repositories/{repository}/branches/{branch}/revert | revert


<a name="createBranch"></a>
# **createBranch**
> String createBranch(repository, BranchCreation)

create branch

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **BranchCreation** | [**BranchCreation**](../Models/BranchCreation.md)|  |

### Return type

[**String**](../Models/string.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: text/html, application/json

<a name="deleteBranch"></a>
# **deleteBranch**
> deleteBranch(repository, branch)

delete branch

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **branch** | **String**|  | [default to null]

### Return type

null (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="diffBranch"></a>
# **diffBranch**
> DiffList diffBranch(repository, branch, after, amount, prefix, delimiter)

diff branch

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **branch** | **String**|  | [default to null]
 **after** | **String**| return items after this value | [optional] [default to null]
 **amount** | **Integer**| how many items to return | [optional] [default to 100]
 **prefix** | **String**| return items prefixed with this value | [optional] [default to null]
 **delimiter** | **String**| delimiter used to group common prefixes by | [optional] [default to null]

### Return type

[**DiffList**](../Models/DiffList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getBranch"></a>
# **getBranch**
> Ref getBranch(repository, branch)

get branch

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **branch** | **String**|  | [default to null]

### Return type

[**Ref**](../Models/Ref.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="listBranches"></a>
# **listBranches**
> RefList listBranches(repository, prefix, after, amount)

list branches

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **prefix** | **String**| return items prefixed with this value | [optional] [default to null]
 **after** | **String**| return items after this value | [optional] [default to null]
 **amount** | **Integer**| how many items to return | [optional] [default to 100]

### Return type

[**RefList**](../Models/RefList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="resetBranch"></a>
# **resetBranch**
> resetBranch(repository, branch, ResetCreation)

reset branch

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **branch** | **String**|  | [default to null]
 **ResetCreation** | [**ResetCreation**](../Models/ResetCreation.md)|  |

### Return type

null (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="revertBranch"></a>
# **revertBranch**
> revertBranch(repository, branch, RevertCreation)

revert

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **branch** | **String**|  | [default to null]
 **RevertCreation** | [**RevertCreation**](../Models/RevertCreation.md)|  |

### Return type

null (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

