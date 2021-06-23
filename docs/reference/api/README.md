# Documentation for lakeFS API

<a name="documentation-for-api-endpoints"></a>
## Documentation for API Endpoints

All URIs are relative to *http://localhost/api/v1*

Class | Method | HTTP request | Description
------------ | ------------- | ------------- | -------------
*ActionsApi* | [**getRun**](apis/ActionsApi.md#getrun) | **GET** /repositories/{repository}/actions/runs/{run_id} | get a run
*ActionsApi* | [**getRunHookOutput**](apis/ActionsApi.md#getrunhookoutput) | **GET** /repositories/{repository}/actions/runs/{run_id}/hooks/{hook_run_id}/output | get run hook output
*ActionsApi* | [**listRepositoryRuns**](apis/ActionsApi.md#listrepositoryruns) | **GET** /repositories/{repository}/actions/runs | list runs
*ActionsApi* | [**listRunHooks**](apis/ActionsApi.md#listrunhooks) | **GET** /repositories/{repository}/actions/runs/{run_id}/hooks | list run hooks
*AuthApi* | [**addGroupMembership**](apis/AuthApi.md#addgroupmembership) | **PUT** /auth/groups/{groupId}/members/{userId} | add group membership
*AuthApi* | [**attachPolicyToGroup**](apis/AuthApi.md#attachpolicytogroup) | **PUT** /auth/groups/{groupId}/policies/{policyId} | attach policy to group
*AuthApi* | [**attachPolicyToUser**](apis/AuthApi.md#attachpolicytouser) | **PUT** /auth/users/{userId}/policies/{policyId} | attach policy to user
*AuthApi* | [**createCredentials**](apis/AuthApi.md#createcredentials) | **POST** /auth/users/{userId}/credentials | create credentials
*AuthApi* | [**createGroup**](apis/AuthApi.md#creategroup) | **POST** /auth/groups | create group
*AuthApi* | [**createPolicy**](apis/AuthApi.md#createpolicy) | **POST** /auth/policies | create policy
*AuthApi* | [**createUser**](apis/AuthApi.md#createuser) | **POST** /auth/users | create user
*AuthApi* | [**deleteCredentials**](apis/AuthApi.md#deletecredentials) | **DELETE** /auth/users/{userId}/credentials/{accessKeyId} | delete credentials
*AuthApi* | [**deleteGroup**](apis/AuthApi.md#deletegroup) | **DELETE** /auth/groups/{groupId} | delete group
*AuthApi* | [**deleteGroupMembership**](apis/AuthApi.md#deletegroupmembership) | **DELETE** /auth/groups/{groupId}/members/{userId} | delete group membership
*AuthApi* | [**deletePolicy**](apis/AuthApi.md#deletepolicy) | **DELETE** /auth/policies/{policyId} | delete policy
*AuthApi* | [**deleteUser**](apis/AuthApi.md#deleteuser) | **DELETE** /auth/users/{userId} | delete user
*AuthApi* | [**detachPolicyFromGroup**](apis/AuthApi.md#detachpolicyfromgroup) | **DELETE** /auth/groups/{groupId}/policies/{policyId} | detach policy from group
*AuthApi* | [**detachPolicyFromUser**](apis/AuthApi.md#detachpolicyfromuser) | **DELETE** /auth/users/{userId}/policies/{policyId} | detach policy from user
*AuthApi* | [**getCredentials**](apis/AuthApi.md#getcredentials) | **GET** /auth/users/{userId}/credentials/{accessKeyId} | get credentials
*AuthApi* | [**getCurrentUser**](apis/AuthApi.md#getcurrentuser) | **GET** /user | get current user
*AuthApi* | [**getGroup**](apis/AuthApi.md#getgroup) | **GET** /auth/groups/{groupId} | get group
*AuthApi* | [**getPolicy**](apis/AuthApi.md#getpolicy) | **GET** /auth/policies/{policyId} | get policy
*AuthApi* | [**getUser**](apis/AuthApi.md#getuser) | **GET** /auth/users/{userId} | get user
*AuthApi* | [**listGroupMembers**](apis/AuthApi.md#listgroupmembers) | **GET** /auth/groups/{groupId}/members | list group members
*AuthApi* | [**listGroupPolicies**](apis/AuthApi.md#listgrouppolicies) | **GET** /auth/groups/{groupId}/policies | list group policies
*AuthApi* | [**listGroups**](apis/AuthApi.md#listgroups) | **GET** /auth/groups | list groups
*AuthApi* | [**listPolicies**](apis/AuthApi.md#listpolicies) | **GET** /auth/policies | list policies
*AuthApi* | [**listUserCredentials**](apis/AuthApi.md#listusercredentials) | **GET** /auth/users/{userId}/credentials | list user credentials
*AuthApi* | [**listUserGroups**](apis/AuthApi.md#listusergroups) | **GET** /auth/users/{userId}/groups | list user groups
*AuthApi* | [**listUserPolicies**](apis/AuthApi.md#listuserpolicies) | **GET** /auth/users/{userId}/policies | list user policies
*AuthApi* | [**listUsers**](apis/AuthApi.md#listusers) | **GET** /auth/users | list users
*AuthApi* | [**login**](apis/AuthApi.md#login) | **POST** /auth/login | perform a login
*AuthApi* | [**logout**](apis/AuthApi.md#logout) | **POST** /auth/logout | logs out a cookie-authenticated user
*AuthApi* | [**updatePolicy**](apis/AuthApi.md#updatepolicy) | **PUT** /auth/policies/{policyId} | update policy
*BranchesApi* | [**createBranch**](apis/BranchesApi.md#createbranch) | **POST** /repositories/{repository}/branches | create branch
*BranchesApi* | [**deleteBranch**](apis/BranchesApi.md#deletebranch) | **DELETE** /repositories/{repository}/branches/{branch} | delete branch
*BranchesApi* | [**diffBranch**](apis/BranchesApi.md#diffbranch) | **GET** /repositories/{repository}/branches/{branch}/diff | diff branch
*BranchesApi* | [**getBranch**](apis/BranchesApi.md#getbranch) | **GET** /repositories/{repository}/branches/{branch} | get branch
*BranchesApi* | [**listBranches**](apis/BranchesApi.md#listbranches) | **GET** /repositories/{repository}/branches | list branches
*BranchesApi* | [**resetBranch**](apis/BranchesApi.md#resetbranch) | **PUT** /repositories/{repository}/branches/{branch} | reset branch
*BranchesApi* | [**revertBranch**](apis/BranchesApi.md#revertbranch) | **POST** /repositories/{repository}/branches/{branch}/revert | revert
*CommitsApi* | [**commit**](apis/CommitsApi.md#commit) | **POST** /repositories/{repository}/branches/{branch}/commits | create commit
*CommitsApi* | [**getCommit**](apis/CommitsApi.md#getcommit) | **GET** /repositories/{repository}/commits/{commitId} | get commit
*CommitsApi* | [**logBranchCommits**](apis/CommitsApi.md#logbranchcommits) | **GET** /repositories/{repository}/branches/{branch}/commits | get commit log from branch. Deprecated: replaced by logCommits by passing branch name as ref 
*ConfigApi* | [**getLakeFSVersion**](apis/ConfigApi.md#getlakefsversion) | **GET** /config/version | get version of lakeFS server
*ConfigApi* | [**getStorageConfig**](apis/ConfigApi.md#getstorageconfig) | **GET** /config/storage | retrieve lakeFS storage configuration
*ConfigApi* | [**setup**](apis/ConfigApi.md#setup) | **POST** /setup_lakefs | setup lakeFS and create a first user
*HealthCheckApi* | [**healthCheck**](apis/HealthCheckApi.md#healthcheck) | **GET** /healthcheck | check that the API server is up and running
*MetadataApi* | [**createSymlinkFile**](apis/MetadataApi.md#createsymlinkfile) | **POST** /repositories/{repository}/refs/{branch}/symlink | creates symlink files corresponding to the given directory
*MetadataApi* | [**getMetaRange**](apis/MetadataApi.md#getmetarange) | **GET** /repositories/{repository}/metadata/meta_range/{meta_range} | return URI to a meta-range file
*MetadataApi* | [**getRange**](apis/MetadataApi.md#getrange) | **GET** /repositories/{repository}/metadata/range/{range} | return URI to a range file
*ObjectsApi* | [**deleteObject**](apis/ObjectsApi.md#deleteobject) | **DELETE** /repositories/{repository}/branches/{branch}/objects | delete object
*ObjectsApi* | [**getObject**](apis/ObjectsApi.md#getobject) | **GET** /repositories/{repository}/refs/{ref}/objects | get object content
*ObjectsApi* | [**getUnderlyingProperties**](apis/ObjectsApi.md#getunderlyingproperties) | **GET** /repositories/{repository}/refs/{ref}/objects/underlyingProperties | get object properties on underlying storage
*ObjectsApi* | [**listObjects**](apis/ObjectsApi.md#listobjects) | **GET** /repositories/{repository}/refs/{ref}/objects/ls | list objects under a given prefix
*ObjectsApi* | [**stageObject**](apis/ObjectsApi.md#stageobject) | **PUT** /repositories/{repository}/branches/{branch}/objects | stage an object\"s metadata for the given branch
*ObjectsApi* | [**statObject**](apis/ObjectsApi.md#statobject) | **GET** /repositories/{repository}/refs/{ref}/objects/stat | get object metadata
*ObjectsApi* | [**uploadObject**](apis/ObjectsApi.md#uploadobject) | **POST** /repositories/{repository}/branches/{branch}/objects | 
*RefsApi* | [**diffRefs**](apis/RefsApi.md#diffrefs) | **GET** /repositories/{repository}/refs/{leftRef}/diff/{rightRef} | diff references
*RefsApi* | [**dumpRefs**](apis/RefsApi.md#dumprefs) | **PUT** /repositories/{repository}/refs/dump | Dump repository refs (tags, commits, branches) to object store
*RefsApi* | [**logCommits**](apis/RefsApi.md#logcommits) | **GET** /repositories/{repository}/refs/{ref}/commits | get commit log from ref
*RefsApi* | [**mergeIntoBranch**](apis/RefsApi.md#mergeintobranch) | **POST** /repositories/{repository}/refs/{sourceRef}/merge/{destinationBranch} | merge references
*RefsApi* | [**restoreRefs**](apis/RefsApi.md#restorerefs) | **PUT** /repositories/{repository}/refs/restore | Restore repository refs (tags, commits, branches) from object store
*RepositoriesApi* | [**createRepository**](apis/RepositoriesApi.md#createrepository) | **POST** /repositories | create repository
*RepositoriesApi* | [**deleteRepository**](apis/RepositoriesApi.md#deleterepository) | **DELETE** /repositories/{repository} | delete repository
*RepositoriesApi* | [**getRepository**](apis/RepositoriesApi.md#getrepository) | **GET** /repositories/{repository} | get repository
*RepositoriesApi* | [**listRepositories**](apis/RepositoriesApi.md#listrepositories) | **GET** /repositories | list repositories
*RetentionApi* | [**getGarbageCollectionRules**](apis/RetentionApi.md#getgarbagecollectionrules) | **GET** /repositories/{repository}/gc/rules | 
*RetentionApi* | [**prepareGarbageCollectionCommits**](apis/RetentionApi.md#preparegarbagecollectioncommits) | **POST** /repositories/{repository}/gc/prepare_commits | save lists of active and expired commits for garbage collection
*RetentionApi* | [**set garbage collection rules**](apis/RetentionApi.md#set garbage collection rules) | **POST** /repositories/{repository}/gc/rules | 
*StagingApi* | [**getPhysicalAddress**](apis/StagingApi.md#getphysicaladdress) | **GET** /repositories/{repository}/branches/{branch}/staging/backing | get a physical address and a return token to write object to underlying storage
*StagingApi* | [**linkPhysicalAddress**](apis/StagingApi.md#linkphysicaladdress) | **PUT** /repositories/{repository}/branches/{branch}/staging/backing | associate staging on this physical address with a path
*TagsApi* | [**createTag**](apis/TagsApi.md#createtag) | **POST** /repositories/{repository}/tags | create tag
*TagsApi* | [**deleteTag**](apis/TagsApi.md#deletetag) | **DELETE** /repositories/{repository}/tags/{tag} | delete tag
*TagsApi* | [**getTag**](apis/TagsApi.md#gettag) | **GET** /repositories/{repository}/tags/{tag} | get tag
*TagsApi* | [**listTags**](apis/TagsApi.md#listtags) | **GET** /repositories/{repository}/tags | list tags


<a name="documentation-for-models"></a>
## Documentation for Models

 - [AccessKeyCredentials](./Models/AccessKeyCredentials.md)
 - [ActionRun](./Models/ActionRun.md)
 - [ActionRunList](./Models/ActionRunList.md)
 - [AuthenticationToken](./Models/AuthenticationToken.md)
 - [BranchCreation](./Models/BranchCreation.md)
 - [Commit](./Models/Commit.md)
 - [CommitCreation](./Models/CommitCreation.md)
 - [CommitList](./Models/CommitList.md)
 - [Credentials](./Models/Credentials.md)
 - [CredentialsList](./Models/CredentialsList.md)
 - [CredentialsWithSecret](./Models/CredentialsWithSecret.md)
 - [CurrentUser](./Models/CurrentUser.md)
 - [Diff](./Models/Diff.md)
 - [DiffList](./Models/DiffList.md)
 - [Error](./Models/Error.md)
 - [GarbageCollectionPrepareRequest](./Models/GarbageCollectionPrepareRequest.md)
 - [GarbageCollectionPrepareResponse](./Models/GarbageCollectionPrepareResponse.md)
 - [GarbageCollectionRule](./Models/GarbageCollectionRule.md)
 - [GarbageCollectionRules](./Models/GarbageCollectionRules.md)
 - [Group](./Models/Group.md)
 - [GroupCreation](./Models/GroupCreation.md)
 - [GroupList](./Models/GroupList.md)
 - [HookRun](./Models/HookRun.md)
 - [HookRunList](./Models/HookRunList.md)
 - [LoginInformation](./Models/LoginInformation.md)
 - [Merge](./Models/Merge.md)
 - [MergeResult](./Models/MergeResult.md)
 - [MergeResultSummary](./Models/MergeResultSummary.md)
 - [ObjectStageCreation](./Models/ObjectStageCreation.md)
 - [ObjectStats](./Models/ObjectStats.md)
 - [ObjectStatsList](./Models/ObjectStatsList.md)
 - [Pagination](./Models/Pagination.md)
 - [Policy](./Models/Policy.md)
 - [PolicyList](./Models/PolicyList.md)
 - [Ref](./Models/Ref.md)
 - [RefList](./Models/RefList.md)
 - [RefsDump](./Models/RefsDump.md)
 - [Repository](./Models/Repository.md)
 - [RepositoryCreation](./Models/RepositoryCreation.md)
 - [RepositoryList](./Models/RepositoryList.md)
 - [ResetCreation](./Models/ResetCreation.md)
 - [RevertCreation](./Models/RevertCreation.md)
 - [Setup](./Models/Setup.md)
 - [StagingLocation](./Models/StagingLocation.md)
 - [StagingMetadata](./Models/StagingMetadata.md)
 - [Statement](./Models/Statement.md)
 - [StorageConfig](./Models/StorageConfig.md)
 - [StorageURI](./Models/StorageURI.md)
 - [TagCreation](./Models/TagCreation.md)
 - [UnderlyingObjectProperties](./Models/UnderlyingObjectProperties.md)
 - [User](./Models/User.md)
 - [UserCreation](./Models/UserCreation.md)
 - [UserList](./Models/UserList.md)
 - [VersionConfig](./Models/VersionConfig.md)


<a name="documentation-for-authorization"></a>
## Documentation for Authorization

<a name="basic_auth"></a>
### basic_auth

- **Type**: HTTP basic authentication

<a name="cookie_auth"></a>
### cookie_auth

- **Type**: API key
- **API key parameter name**: access_token
- **Location**: 

<a name="jwt_token"></a>
### jwt_token

- **Type**: HTTP basic authentication

