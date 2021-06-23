# AuthApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**addGroupMembership**](AuthApi.md#addGroupMembership) | **PUT** /auth/groups/{groupId}/members/{userId} | add group membership
[**attachPolicyToGroup**](AuthApi.md#attachPolicyToGroup) | **PUT** /auth/groups/{groupId}/policies/{policyId} | attach policy to group
[**attachPolicyToUser**](AuthApi.md#attachPolicyToUser) | **PUT** /auth/users/{userId}/policies/{policyId} | attach policy to user
[**createCredentials**](AuthApi.md#createCredentials) | **POST** /auth/users/{userId}/credentials | create credentials
[**createGroup**](AuthApi.md#createGroup) | **POST** /auth/groups | create group
[**createPolicy**](AuthApi.md#createPolicy) | **POST** /auth/policies | create policy
[**createUser**](AuthApi.md#createUser) | **POST** /auth/users | create user
[**deleteCredentials**](AuthApi.md#deleteCredentials) | **DELETE** /auth/users/{userId}/credentials/{accessKeyId} | delete credentials
[**deleteGroup**](AuthApi.md#deleteGroup) | **DELETE** /auth/groups/{groupId} | delete group
[**deleteGroupMembership**](AuthApi.md#deleteGroupMembership) | **DELETE** /auth/groups/{groupId}/members/{userId} | delete group membership
[**deletePolicy**](AuthApi.md#deletePolicy) | **DELETE** /auth/policies/{policyId} | delete policy
[**deleteUser**](AuthApi.md#deleteUser) | **DELETE** /auth/users/{userId} | delete user
[**detachPolicyFromGroup**](AuthApi.md#detachPolicyFromGroup) | **DELETE** /auth/groups/{groupId}/policies/{policyId} | detach policy from group
[**detachPolicyFromUser**](AuthApi.md#detachPolicyFromUser) | **DELETE** /auth/users/{userId}/policies/{policyId} | detach policy from user
[**getCredentials**](AuthApi.md#getCredentials) | **GET** /auth/users/{userId}/credentials/{accessKeyId} | get credentials
[**getCurrentUser**](AuthApi.md#getCurrentUser) | **GET** /user | get current user
[**getGroup**](AuthApi.md#getGroup) | **GET** /auth/groups/{groupId} | get group
[**getPolicy**](AuthApi.md#getPolicy) | **GET** /auth/policies/{policyId} | get policy
[**getUser**](AuthApi.md#getUser) | **GET** /auth/users/{userId} | get user
[**listGroupMembers**](AuthApi.md#listGroupMembers) | **GET** /auth/groups/{groupId}/members | list group members
[**listGroupPolicies**](AuthApi.md#listGroupPolicies) | **GET** /auth/groups/{groupId}/policies | list group policies
[**listGroups**](AuthApi.md#listGroups) | **GET** /auth/groups | list groups
[**listPolicies**](AuthApi.md#listPolicies) | **GET** /auth/policies | list policies
[**listUserCredentials**](AuthApi.md#listUserCredentials) | **GET** /auth/users/{userId}/credentials | list user credentials
[**listUserGroups**](AuthApi.md#listUserGroups) | **GET** /auth/users/{userId}/groups | list user groups
[**listUserPolicies**](AuthApi.md#listUserPolicies) | **GET** /auth/users/{userId}/policies | list user policies
[**listUsers**](AuthApi.md#listUsers) | **GET** /auth/users | list users
[**login**](AuthApi.md#login) | **POST** /auth/login | perform a login
[**logout**](AuthApi.md#logout) | **POST** /auth/logout | logs out a cookie-authenticated user
[**updatePolicy**](AuthApi.md#updatePolicy) | **PUT** /auth/policies/{policyId} | update policy


<a name="addGroupMembership"></a>
# **addGroupMembership**
> addGroupMembership(groupId, userId)

add group membership

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **groupId** | **String**|  | [default to null]
 **userId** | **String**|  | [default to null]

### Return type

null (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="attachPolicyToGroup"></a>
# **attachPolicyToGroup**
> attachPolicyToGroup(groupId, policyId)

attach policy to group

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **groupId** | **String**|  | [default to null]
 **policyId** | **String**|  | [default to null]

### Return type

null (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="attachPolicyToUser"></a>
# **attachPolicyToUser**
> attachPolicyToUser(userId, policyId)

attach policy to user

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **userId** | **String**|  | [default to null]
 **policyId** | **String**|  | [default to null]

### Return type

null (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="createCredentials"></a>
# **createCredentials**
> CredentialsWithSecret createCredentials(userId)

create credentials

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **userId** | **String**|  | [default to null]

### Return type

[**CredentialsWithSecret**](../Models/CredentialsWithSecret.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="createGroup"></a>
# **createGroup**
> Group createGroup(GroupCreation)

create group

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **GroupCreation** | [**GroupCreation**](../Models/GroupCreation.md)|  | [optional]

### Return type

[**Group**](../Models/Group.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="createPolicy"></a>
# **createPolicy**
> Policy createPolicy(Policy)

create policy

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **Policy** | [**Policy**](../Models/Policy.md)|  |

### Return type

[**Policy**](../Models/Policy.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="createUser"></a>
# **createUser**
> User createUser(UserCreation)

create user

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **UserCreation** | [**UserCreation**](../Models/UserCreation.md)|  | [optional]

### Return type

[**User**](../Models/User.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="deleteCredentials"></a>
# **deleteCredentials**
> deleteCredentials(userId, accessKeyId)

delete credentials

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **userId** | **String**|  | [default to null]
 **accessKeyId** | **String**|  | [default to null]

### Return type

null (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="deleteGroup"></a>
# **deleteGroup**
> deleteGroup(groupId)

delete group

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **groupId** | **String**|  | [default to null]

### Return type

null (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="deleteGroupMembership"></a>
# **deleteGroupMembership**
> deleteGroupMembership(groupId, userId)

delete group membership

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **groupId** | **String**|  | [default to null]
 **userId** | **String**|  | [default to null]

### Return type

null (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="deletePolicy"></a>
# **deletePolicy**
> deletePolicy(policyId)

delete policy

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **policyId** | **String**|  | [default to null]

### Return type

null (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="deleteUser"></a>
# **deleteUser**
> deleteUser(userId)

delete user

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **userId** | **String**|  | [default to null]

### Return type

null (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="detachPolicyFromGroup"></a>
# **detachPolicyFromGroup**
> detachPolicyFromGroup(groupId, policyId)

detach policy from group

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **groupId** | **String**|  | [default to null]
 **policyId** | **String**|  | [default to null]

### Return type

null (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="detachPolicyFromUser"></a>
# **detachPolicyFromUser**
> detachPolicyFromUser(userId, policyId)

detach policy from user

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **userId** | **String**|  | [default to null]
 **policyId** | **String**|  | [default to null]

### Return type

null (empty response body)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getCredentials"></a>
# **getCredentials**
> Credentials getCredentials(userId, accessKeyId)

get credentials

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **userId** | **String**|  | [default to null]
 **accessKeyId** | **String**|  | [default to null]

### Return type

[**Credentials**](../Models/Credentials.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getCurrentUser"></a>
# **getCurrentUser**
> CurrentUser getCurrentUser()

get current user

### Parameters
This endpoint does not need any parameter.

### Return type

[**CurrentUser**](../Models/CurrentUser.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getGroup"></a>
# **getGroup**
> Group getGroup(groupId)

get group

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **groupId** | **String**|  | [default to null]

### Return type

[**Group**](../Models/Group.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getPolicy"></a>
# **getPolicy**
> Policy getPolicy(policyId)

get policy

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **policyId** | **String**|  | [default to null]

### Return type

[**Policy**](../Models/Policy.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getUser"></a>
# **getUser**
> User getUser(userId)

get user

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **userId** | **String**|  | [default to null]

### Return type

[**User**](../Models/User.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="listGroupMembers"></a>
# **listGroupMembers**
> UserList listGroupMembers(groupId, prefix, after, amount)

list group members

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **groupId** | **String**|  | [default to null]
 **prefix** | **String**| return items prefixed with this value | [optional] [default to null]
 **after** | **String**| return items after this value | [optional] [default to null]
 **amount** | **Integer**| how many items to return | [optional] [default to 100]

### Return type

[**UserList**](../Models/UserList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="listGroupPolicies"></a>
# **listGroupPolicies**
> PolicyList listGroupPolicies(groupId, prefix, after, amount)

list group policies

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **groupId** | **String**|  | [default to null]
 **prefix** | **String**| return items prefixed with this value | [optional] [default to null]
 **after** | **String**| return items after this value | [optional] [default to null]
 **amount** | **Integer**| how many items to return | [optional] [default to 100]

### Return type

[**PolicyList**](../Models/PolicyList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="listGroups"></a>
# **listGroups**
> GroupList listGroups(prefix, after, amount)

list groups

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **String**| return items prefixed with this value | [optional] [default to null]
 **after** | **String**| return items after this value | [optional] [default to null]
 **amount** | **Integer**| how many items to return | [optional] [default to 100]

### Return type

[**GroupList**](../Models/GroupList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="listPolicies"></a>
# **listPolicies**
> PolicyList listPolicies(prefix, after, amount)

list policies

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **String**| return items prefixed with this value | [optional] [default to null]
 **after** | **String**| return items after this value | [optional] [default to null]
 **amount** | **Integer**| how many items to return | [optional] [default to 100]

### Return type

[**PolicyList**](../Models/PolicyList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="listUserCredentials"></a>
# **listUserCredentials**
> CredentialsList listUserCredentials(userId, prefix, after, amount)

list user credentials

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **userId** | **String**|  | [default to null]
 **prefix** | **String**| return items prefixed with this value | [optional] [default to null]
 **after** | **String**| return items after this value | [optional] [default to null]
 **amount** | **Integer**| how many items to return | [optional] [default to 100]

### Return type

[**CredentialsList**](../Models/CredentialsList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="listUserGroups"></a>
# **listUserGroups**
> GroupList listUserGroups(userId, prefix, after, amount)

list user groups

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **userId** | **String**|  | [default to null]
 **prefix** | **String**| return items prefixed with this value | [optional] [default to null]
 **after** | **String**| return items after this value | [optional] [default to null]
 **amount** | **Integer**| how many items to return | [optional] [default to 100]

### Return type

[**GroupList**](../Models/GroupList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="listUserPolicies"></a>
# **listUserPolicies**
> PolicyList listUserPolicies(userId, prefix, after, amount, effective)

list user policies

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **userId** | **String**|  | [default to null]
 **prefix** | **String**| return items prefixed with this value | [optional] [default to null]
 **after** | **String**| return items after this value | [optional] [default to null]
 **amount** | **Integer**| how many items to return | [optional] [default to 100]
 **effective** | **Boolean**| will return all distinct policies attached to the user or any of its groups | [optional] [default to false]

### Return type

[**PolicyList**](../Models/PolicyList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="listUsers"></a>
# **listUsers**
> UserList listUsers(prefix, after, amount)

list users

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **String**| return items prefixed with this value | [optional] [default to null]
 **after** | **String**| return items after this value | [optional] [default to null]
 **amount** | **Integer**| how many items to return | [optional] [default to 100]

### Return type

[**UserList**](../Models/UserList.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="login"></a>
# **login**
> AuthenticationToken login(LoginInformation)

perform a login

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **LoginInformation** | [**LoginInformation**](../Models/LoginInformation.md)|  | [optional]

### Return type

[**AuthenticationToken**](../Models/AuthenticationToken.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="logout"></a>
# **logout**
> logout()

logs out a cookie-authenticated user

### Parameters
This endpoint does not need any parameter.

### Return type

null (empty response body)

### Authorization

[cookie_auth](../README.md#cookie_auth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="updatePolicy"></a>
# **updatePolicy**
> Policy updatePolicy(policyId, Policy)

update policy

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **policyId** | **String**|  | [default to null]
 **Policy** | [**Policy**](../Models/Policy.md)|  |

### Return type

[**Policy**](../Models/Policy.md)

### Authorization

[basic_auth](../README.md#basic_auth), [cookie_auth](../README.md#cookie_auth), [jwt_token](../README.md#jwt_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

