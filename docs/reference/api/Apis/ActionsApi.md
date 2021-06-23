# ActionsApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**getRun**](ActionsApi.md#getRun) | **GET** /repositories/{repository}/actions/runs/{run_id} | get a run
[**getRunHookOutput**](ActionsApi.md#getRunHookOutput) | **GET** /repositories/{repository}/actions/runs/{run_id}/hooks/{hook_run_id}/output | get run hook output
[**listRepositoryRuns**](ActionsApi.md#listRepositoryRuns) | **GET** /repositories/{repository}/actions/runs | list runs
[**listRunHooks**](ActionsApi.md#listRunHooks) | **GET** /repositories/{repository}/actions/runs/{run_id}/hooks | list run hooks


<a name="getRun"></a>
# **getRun**
> ActionRun getRun(repository, run\_id)

get a run

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **run\_id** | **String**|  | [default to null]

### Return type

[**ActionRun**](../Models/ActionRun.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getRunHookOutput"></a>
# **getRunHookOutput**
> File getRunHookOutput(repository, run\_id, hook\_run\_id)

get run hook output

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **run\_id** | **String**|  | [default to null]
 **hook\_run\_id** | **String**|  | [default to null]

### Return type

[**File**](../Models/file.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/octet-stream, application/json

<a name="listRepositoryRuns"></a>
# **listRepositoryRuns**
> ActionRunList listRepositoryRuns(repository, after, amount, branch, commit)

list runs

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **after** | **String**| return items after this value | [optional] [default to null]
 **amount** | **Integer**| how many items to return | [optional] [default to 100]
 **branch** | **String**|  | [optional] [default to null]
 **commit** | **String**|  | [optional] [default to null]

### Return type

[**ActionRunList**](../Models/ActionRunList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="listRunHooks"></a>
# **listRunHooks**
> HookRunList listRunHooks(repository, run\_id, after, amount)

list run hooks

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **repository** | **String**|  | [default to null]
 **run\_id** | **String**|  | [default to null]
 **after** | **String**| return items after this value | [optional] [default to null]
 **amount** | **Integer**| how many items to return | [optional] [default to 100]

### Return type

[**HookRunList**](../Models/HookRunList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

