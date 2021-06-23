# RefsApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**diffRefs**](RefsApi.md#diffRefs) | **GET** /repositories/{repository}/refs/{leftRef}/diff/{rightRef} | diff references
[**dumpRefs**](RefsApi.md#dumpRefs) | **PUT** /repositories/{repository}/refs/dump | Dump repository refs (tags, commits, branches) to object store
[**logCommits**](RefsApi.md#logCommits) | **GET** /repositories/{repository}/refs/{ref}/commits | get commit log from ref
[**mergeIntoBranch**](RefsApi.md#mergeIntoBranch) | **POST** /repositories/{repository}/refs/{sourceRef}/merge/{destinationBranch} | merge references
[**restoreRefs**](RefsApi.md#restoreRefs) | **PUT** /repositories/{repository}/refs/restore | Restore repository refs (tags, commits, branches) from object store


<a name="diffRefs"></a>
# **diffRefs**
> DiffList diffRefs(repository, leftRef, rightRef, after, amount, prefix, delimiter, type, diff\_type)

diff references

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **leftRef** | **String**| a reference (could be either a branch or a commit ID) | [default to null]
 **rightRef** | **String**| a reference (could be either a branch or a commit ID) to compare against | [default to null]
 **after** | **String**| return items after this value | [optional] [default to null]
 **amount** | **Integer**| how many items to return | [optional] [default to 100]
 **prefix** | **String**| return items prefixed with this value | [optional] [default to null]
 **delimiter** | **String**| delimiter used to group common prefixes by | [optional] [default to null]
 **type** | **String**|  | [optional] [default to null]
 **diff\_type** | **String**|  | [optional] [default to three_dot] [enum: two_dot, three_dot]

### Return type

[**DiffList**](../Models/DiffList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="dumpRefs"></a>
# **dumpRefs**
> RefsDump dumpRefs(repository)

Dump repository refs (tags, commits, branches) to object store

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]

### Return type

[**RefsDump**](../Models/RefsDump.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="logCommits"></a>
# **logCommits**
> CommitList logCommits(repository, ref, after, amount)

get commit log from ref

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **ref** | **String**|  | [default to null]
 **after** | **String**| return items after this value | [optional] [default to null]
 **amount** | **Integer**| how many items to return | [optional] [default to 100]

### Return type

[**CommitList**](../Models/CommitList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="mergeIntoBranch"></a>
# **mergeIntoBranch**
> MergeResult mergeIntoBranch(repository, sourceRef, destinationBranch, Merge)

merge references

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **sourceRef** | **String**| source ref | [default to null]
 **destinationBranch** | **String**| destination branch name | [default to null]
 **Merge** | [**Merge**](../Models/Merge.md)|  | [optional]

### Return type

[**MergeResult**](../Models/MergeResult.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="restoreRefs"></a>
# **restoreRefs**
> restoreRefs(repository, RefsDump)

Restore repository refs (tags, commits, branches) from object store

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **RefsDump** | [**RefsDump**](../Models/RefsDump.md)|  |

### Return type

null (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

