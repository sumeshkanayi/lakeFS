# Documentation for lakeFS API

<a name="documentation-for-api-endpoints"></a>
## Documentation for API Endpoints

All URIs are relative to *http://localhost/api/v1*

Class | Method | HTTP request | Description
------------ | ------------- | ------------- | -------------
*ActionsApi* | [**getRun**](swagger/Apis/ActionsApi.md#getrun) | **GET** /repositories/{repository}/actions/runs/{run_id} | get a run
*ActionsApi* | [**getRunHookOutput**](Apis/ActionsApi.md#getrunhookoutput) | **GET** /repositories/{repository}/actions/runs/{run_id}/hooks/{hook_run_id}/output | get run hook output
*ActionsApi* | [**listRepositoryRuns**](Apis/ActionsApi.md#listrepositoryruns) | **GET** /repositories/{repository}/actions/runs | list runs
*ActionsApi* | [**listRunHooks**](Apis/ActionsApi.md#listrunhooks) | **GET** /repositories/{repository}/actions/runs/{run_id}/hooks | list run hooks
*AuthApi* | [**addGroupMembership**](Apis/AuthApi.md#addgroupmembership) | **PUT** /auth/groups/{groupId}/members/{userId} | add group membership
*AuthApi* | [**attachPolicyToGroup**](Apis/AuthApi.md#attachpolicytogroup) | **PUT** /auth/groups/{groupId}/policies/{policyId} | attach policy to group
*AuthApi* | [**attachPolicyToUser**](Apis/AuthApi.md#attachpolicytouser) | **PUT** /auth/users/{userId}/policies/{policyId} | attach policy to user
*AuthApi* | [**createCredentials**](Apis/AuthApi.md#createcredentials) | **POST** /auth/users/{userId}/credentials | create credentials
*AuthApi* | [**createGroup**](Apis/AuthApi.md#creategroup) | **POST** /auth/groups | create group
*AuthApi* | [**createPolicy**](Apis/AuthApi.md#createpolicy) | **POST** /auth/policies | create policy
*AuthApi* | [**createUser**](Apis/AuthApi.md#createuser) | **POST** /auth/users | create user
*AuthApi* | [**deleteCredentials**](Apis/AuthApi.md#deletecredentials) | **DELETE** /auth/users/{userId}/credentials/{accessKeyId} | delete credentials
*AuthApi* | [**deleteGroup**](Apis/AuthApi.md#deletegroup) | **DELETE** /auth/groups/{groupId} | delete group
*AuthApi* | [**deleteGroupMembership**](Apis/AuthApi.md#deletegroupmembership) | **DELETE** /auth/groups/{groupId}/members/{userId} | delete group membership
*AuthApi* | [**deletePolicy**](Apis/AuthApi.md#deletepolicy) | **DELETE** /auth/policies/{policyId} | delete policy
*AuthApi* | [**deleteUser**](Apis/AuthApi.md#deleteuser) | **DELETE** /auth/users/{userId} | delete user
*AuthApi* | [**detachPolicyFromGroup**](Apis/AuthApi.md#detachpolicyfromgroup) | **DELETE** /auth/groups/{groupId}/policies/{policyId} | detach policy from group
*AuthApi* | [**detachPolicyFromUser**](Apis/AuthApi.md#detachpolicyfromuser) | **DELETE** /auth/users/{userId}/policies/{policyId} | detach policy from user
*AuthApi* | [**getCredentials**](Apis/AuthApi.md#getcredentials) | **GET** /auth/users/{userId}/credentials/{accessKeyId} | get credentials
*AuthApi* | [**getCurrentUser**](Apis/AuthApi.md#getcurrentuser) | **GET** /user | get current user
*AuthApi* | [**getGroup**](Apis/AuthApi.md#getgroup) | **GET** /auth/groups/{groupId} | get group
*AuthApi* | [**getPolicy**](Apis/AuthApi.md#getpolicy) | **GET** /auth/policies/{policyId} | get policy
*AuthApi* | [**getUser**](Apis/AuthApi.md#getuser) | **GET** /auth/users/{userId} | get user
*AuthApi* | [**listGroupMembers**](Apis/AuthApi.md#listgroupmembers) | **GET** /auth/groups/{groupId}/members | list group members
*AuthApi* | [**listGroupPolicies**](Apis/AuthApi.md#listgrouppolicies) | **GET** /auth/groups/{groupId}/policies | list group policies
*AuthApi* | [**listGroups**](Apis/AuthApi.md#listgroups) | **GET** /auth/groups | list groups
*AuthApi* | [**listPolicies**](Apis/AuthApi.md#listpolicies) | **GET** /auth/policies | list policies
*AuthApi* | [**listUserCredentials**](Apis/AuthApi.md#listusercredentials) | **GET** /auth/users/{userId}/credentials | list user credentials
*AuthApi* | [**listUserGroups**](Apis/AuthApi.md#listusergroups) | **GET** /auth/users/{userId}/groups | list user groups
*AuthApi* | [**listUserPolicies**](Apis/AuthApi.md#listuserpolicies) | **GET** /auth/users/{userId}/policies | list user policies
*AuthApi* | [**listUsers**](Apis/AuthApi.md#listusers) | **GET** /auth/users | list users
*AuthApi* | [**login**](Apis/AuthApi.md#login) | **POST** /auth/login | perform a login
*AuthApi* | [**logout**](Apis/AuthApi.md#logout) | **POST** /auth/logout | logs out a cookie-authenticated user
*AuthApi* | [**updatePolicy**](Apis/AuthApi.md#updatepolicy) | **PUT** /auth/policies/{policyId} | update policy
*BranchesApi* | [**createBranch**](Apis/BranchesApi.md#createbranch) | **POST** /repositories/{repository}/branches | create branch
*BranchesApi* | [**deleteBranch**](Apis/BranchesApi.md#deletebranch) | **DELETE** /repositories/{repository}/branches/{branch} | delete branch
*BranchesApi* | [**diffBranch**](Apis/BranchesApi.md#diffbranch) | **GET** /repositories/{repository}/branches/{branch}/diff | diff branch
*BranchesApi* | [**getBranch**](Apis/BranchesApi.md#getbranch) | **GET** /repositories/{repository}/branches/{branch} | get branch
*BranchesApi* | [**listBranches**](Apis/BranchesApi.md#listbranches) | **GET** /repositories/{repository}/branches | list branches
*BranchesApi* | [**resetBranch**](Apis/BranchesApi.md#resetbranch) | **PUT** /repositories/{repository}/branches/{branch} | reset branch
*BranchesApi* | [**revertBranch**](Apis/BranchesApi.md#revertbranch) | **POST** /repositories/{repository}/branches/{branch}/revert | revert
*CommitsApi* | [**commit**](Apis/CommitsApi.md#commit) | **POST** /repositories/{repository}/branches/{branch}/commits | create commit
*CommitsApi* | [**getCommit**](Apis/CommitsApi.md#getcommit) | **GET** /repositories/{repository}/commits/{commitId} | get commit
*CommitsApi* | [**logBranchCommits**](Apis/CommitsApi.md#logbranchcommits) | **GET** /repositories/{repository}/branches/{branch}/commits | get commit log from branch. Deprecated: replaced by logCommits by passing branch name as ref 
*ConfigApi* | [**getLakeFSVersion**](Apis/ConfigApi.md#getlakefsversion) | **GET** /config/version | get version of lakeFS server
*ConfigApi* | [**getStorageConfig**](Apis/ConfigApi.md#getstorageconfig) | **GET** /config/storage | retrieve lakeFS storage configuration
*ConfigApi* | [**setup**](Apis/ConfigApi.md#setup) | **POST** /setup_lakefs | setup lakeFS and create a first user
*HealthCheckApi* | [**healthCheck**](Apis/HealthCheckApi.md#healthcheck) | **GET** /healthcheck | check that the API server is up and running
*MetadataApi* | [**createSymlinkFile**](Apis/MetadataApi.md#createsymlinkfile) | **POST** /repositories/{repository}/refs/{branch}/symlink | creates symlink files corresponding to the given directory
*MetadataApi* | [**getMetaRange**](Apis/MetadataApi.md#getmetarange) | **GET** /repositories/{repository}/metadata/meta_range/{meta_range} | return URI to a meta-range file
*MetadataApi* | [**getRange**](Apis/MetadataApi.md#getrange) | **GET** /repositories/{repository}/metadata/range/{range} | return URI to a range file
*ObjectsApi* | [**deleteObject**](Apis/ObjectsApi.md#deleteobject) | **DELETE** /repositories/{repository}/branches/{branch}/objects | delete object
*ObjectsApi* | [**getObject**](Apis/ObjectsApi.md#getobject) | **GET** /repositories/{repository}/refs/{ref}/objects | get object content
*ObjectsApi* | [**getUnderlyingProperties**](Apis/ObjectsApi.md#getunderlyingproperties) | **GET** /repositories/{repository}/refs/{ref}/objects/underlyingProperties | get object properties on underlying storage
*ObjectsApi* | [**listObjects**](Apis/ObjectsApi.md#listobjects) | **GET** /repositories/{repository}/refs/{ref}/objects/ls | list objects under a given prefix
*ObjectsApi* | [**stageObject**](Apis/ObjectsApi.md#stageobject) | **PUT** /repositories/{repository}/branches/{branch}/objects | stage an object\"s metadata for the given branch
*ObjectsApi* | [**statObject**](Apis/ObjectsApi.md#statobject) | **GET** /repositories/{repository}/refs/{ref}/objects/stat | get object metadata
*ObjectsApi* | [**uploadObject**](Apis/ObjectsApi.md#uploadobject) | **POST** /repositories/{repository}/branches/{branch}/objects | 
*RefsApi* | [**diffRefs**](Apis/RefsApi.md#diffrefs) | **GET** /repositories/{repository}/refs/{leftRef}/diff/{rightRef} | diff references
*RefsApi* | [**dumpRefs**](Apis/RefsApi.md#dumprefs) | **PUT** /repositories/{repository}/refs/dump | Dump repository refs (tags, commits, branches) to object store
*RefsApi* | [**logCommits**](Apis/RefsApi.md#logcommits) | **GET** /repositories/{repository}/refs/{ref}/commits | get commit log from ref
*RefsApi* | [**mergeIntoBranch**](Apis/RefsApi.md#mergeintobranch) | **POST** /repositories/{repository}/refs/{sourceRef}/merge/{destinationBranch} | merge references
*RefsApi* | [**restoreRefs**](Apis/RefsApi.md#restorerefs) | **PUT** /repositories/{repository}/refs/restore | Restore repository refs (tags, commits, branches) from object store
*RepositoriesApi* | [**createRepository**](Apis/RepositoriesApi.md#createrepository) | **POST** /repositories | create repository
*RepositoriesApi* | [**deleteRepository**](Apis/RepositoriesApi.md#deleterepository) | **DELETE** /repositories/{repository} | delete repository
*RepositoriesApi* | [**getRepository**](Apis/RepositoriesApi.md#getrepository) | **GET** /repositories/{repository} | get repository
*RepositoriesApi* | [**listRepositories**](Apis/RepositoriesApi.md#listrepositories) | **GET** /repositories | list repositories
*RetentionApi* | [**getGarbageCollectionRules**](Apis/RetentionApi.md#getgarbagecollectionrules) | **GET** /repositories/{repository}/gc/rules | 
*RetentionApi* | [**prepareGarbageCollectionCommits**](Apis/RetentionApi.md#preparegarbagecollectioncommits) | **POST** /repositories/{repository}/gc/prepare_commits | save lists of active and expired commits for garbage collection
*RetentionApi* | [**set garbage collection rules**](Apis/RetentionApi.md#set garbage collection rules) | **POST** /repositories/{repository}/gc/rules | 
*StagingApi* | [**getPhysicalAddress**](Apis/StagingApi.md#getphysicaladdress) | **GET** /repositories/{repository}/branches/{branch}/staging/backing | get a physical address and a return token to write object to underlying storage
*StagingApi* | [**linkPhysicalAddress**](Apis/StagingApi.md#linkphysicaladdress) | **PUT** /repositories/{repository}/branches/{branch}/staging/backing | associate staging on this physical address with a path
*TagsApi* | [**createTag**](Apis/TagsApi.md#createtag) | **POST** /repositories/{repository}/tags | create tag
*TagsApi* | [**deleteTag**](Apis/TagsApi.md#deletetag) | **DELETE** /repositories/{repository}/tags/{tag} | delete tag
*TagsApi* | [**getTag**](Apis/TagsApi.md#gettag) | **GET** /repositories/{repository}/tags/{tag} | get tag
*TagsApi* | [**listTags**](Apis/TagsApi.md#listtags) | **GET** /repositories/{repository}/tags | list tags


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

