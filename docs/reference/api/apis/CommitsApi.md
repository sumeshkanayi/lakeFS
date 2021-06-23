# CommitsApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**commit**](CommitsApi.md#commit) | **POST** /repositories/{repository}/branches/{branch}/commits | create commit
[**getCommit**](CommitsApi.md#getCommit) | **GET** /repositories/{repository}/commits/{commitId} | get commit
[**logBranchCommits**](CommitsApi.md#logBranchCommits) | **GET** /repositories/{repository}/branches/{branch}/commits | get commit log from branch. Deprecated: replaced by logCommits by passing branch name as ref 


<a name="commit"></a>
# **commit**
> Commit commit(repository, branch, CommitCreation)

create commit

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **branch** | **String**|  | [default to null]
 **CommitCreation** | [**CommitCreation**](../Models/CommitCreation.md)|  |

### Return type

[**Commit**](../Models/Commit.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="getCommit"></a>
# **getCommit**
> Commit getCommit(repository, commitId)

get commit

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **commitId** | **String**|  | [default to null]

### Return type

[**Commit**](../Models/Commit.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="logBranchCommits"></a>
# **logBranchCommits**
> CommitList logBranchCommits(repository, branch, after, amount)

get commit log from branch. Deprecated: replaced by logCommits by passing branch name as ref 

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **branch** | **String**|  | [default to null]
 **after** | **String**| return items after this value | [optional] [default to null]
 **amount** | **Integer**| how many items to return | [optional] [default to 100]

### Return type

[**CommitList**](../Models/CommitList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

