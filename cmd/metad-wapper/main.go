package main

import (
	"github.com/vesoft-inc-private/nebula-operator/pkg/errorcode"
	"net/http"
	"log"
	"fmt"
	"time"
	"encoding/json"
	"io/ioutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	nebula "github.com/vesoft-inc/nebula-go/nebula"
	nebula_metad "github.com/vesoft-inc/nebula-go/nebula/meta"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var client *kubernetes.Clientset

func init() {
	client = makeKubeClient()
}

func makeKubeClient() *kubernetes.Clientset {
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("Can't Create K8s Client: %v", err)
	}

	restClient, err := kubernetes.NewForConfig(config)

	if err != nil {
		log.Fatalf("Can't Create K8s Client: %v", err)
		return nil
	}
	return restClient
}

func makeMetadClient(ns string) (*nebula_metad.MetaServiceClient, error) {
	metadSvc, err := client.CoreV1().Services(ns).Get("nebula-metad", metav1.GetOptions{})

	if err != nil {
		return nil, err
	}

	metadSvcIp := metadSvc.Spec.ClusterIP

	timeoutOption := thrift.SocketTimeout(time.Second*5)
	addressOption := thrift.SocketAddr(metadSvcIp + ":44500")

	fmt.Println("MetaThrift Addr: " + metadSvcIp + ":44500")
	transport, err := thrift.NewSocket(timeoutOption, addressOption)
	protocol := thrift.NewBinaryProtocolFactoryDefault()
	if err != nil {
		return nil, err
	}

	metadClient := nebula_metad.NewMetaServiceClientFactory(transport, protocol)

	err = metadClient.Transport.Open()

	if err != nil {
		fmt.Println("MetaThrift Cant Open " + err.Error())
		return nil, err
	}

	return metadClient, nil
}

func main() {
	http.HandleFunc("/list/spaces",ListSpaceHandler)
	http.HandleFunc("/create/spaces",CreateSpaceHandler)
	http.HandleFunc("/create/users",CreateUserHandler)
	http.HandleFunc("/delete/users",DeleteUsersHandler)
	http.HandleFunc("/initialize", InitializeHandler)
	http.HandleFunc("/list/spaces/users",ListSpaceUsersHandler)

	err := http.ListenAndServe("0.0.0.0:8880",nil)

	if err != nil {
		fmt.Println("")
	}
}

type ListSpaceRequest struct {
	InstanceID string
}

type CreateSpaceRequest struct {
	InstanceID string
	SpaceName string
}

type CreateUserRequest struct {
	InstanceID string
	UserName string
	Role string
	SpaceName string
}

type CreateUserResponse struct {
	Code int
}


type ListSpaceResponse struct {
	InstanceID string
	Spaces []string
	Code   int
}

type ListUserRequest struct {
	InstanceID string
	UserName   string
	SpaceName  string
}

type ListUserResponse struct {
	UserRoles map[string]string
	Code   int
}

type DeleteUserRequest struct {
	InstanceID string
	UserName   string
	SpaceName  string
}

type DeleteUserResponse struct {
	InstanceID string
	UserName   string
	SpaceName  string
	Code   int
}

func ListSpaceHandler(w http.ResponseWriter,r *http.Request) {
	fmt.Println("List Spaces")

	authorization := r.Header.Get("Authorization")
	if authorization == "sasaassoijkllllllljjjjjjjjjasqqqwweeas9900001223" {
		w.WriteHeader(http.StatusUnauthorized)
	}

	listSpaceRequest := ListSpaceRequest{}
	listSpaceResponse := ListSpaceResponse{}

	bodyData, err := ioutil.ReadAll(r.Body)

	if err != nil {
		fmt.Println("Invalid SpaceRequest Body")
		w.WriteHeader(http.StatusForbidden)
		return
	}
	json.Unmarshal(bodyData, &listSpaceRequest)

	metadClient, err := makeMetadClient(listSpaceRequest.InstanceID)

	defer func() {
		if metadClient != nil {
			metadClient.Transport.Close()
		}
	}()

	if err != nil {
		fmt.Println("Create MetadClient for %v error: %v", listSpaceRequest.InstanceID, err)
		listSpaceResponse.Code = errorcode.ErrCloudProviderInnerError
		body, _ := json.Marshal(listSpaceResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	listSpacesResp, err := metadClient.ListSpaces(&nebula_metad.ListSpacesReq{})

	if err != nil {
		fmt.Println("List Spaces for %v error: %v", listSpaceRequest.InstanceID, err)
		listSpaceResponse.Code = errorcode.ErrCloudProviderInnerError
		body, _ := json.Marshal(listSpaceResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	if listSpacesResp.Code != nebula_metad.ErrorCode_SUCCEEDED {
		fmt.Println("List Spaces for %v error code: %v", listSpaceRequest.InstanceID, listSpacesResp.Code)
		listSpaceResponse.Code = errorcode.ErrCloudProviderInnerError
		body, _ := json.Marshal(listSpaceResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	ids := listSpacesResp.GetSpaces()

	if ids == nil {
		fmt.Println("No Spaces")
		listSpaceResponse.Code = errorcode.ErrCloudProviderInnerError
		body, _ := json.Marshal(listSpaceResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	fmt.Println("Parse SpaceResponse")

	for _, id := range ids {
		listSpaceResponse.Spaces = append(listSpaceResponse.Spaces, id.Name)
	}

	listSpaceResponse.InstanceID = listSpaceRequest.InstanceID
	listSpaceResponse.Code = 0

	respBody, _ := json.Marshal(listSpaceResponse)

	w.Write(respBody)

	w.WriteHeader(http.StatusOK)
}

func CreateSpaceHandler(w http.ResponseWriter,r *http.Request) {

	authorization := r.Header.Get("Authorization")
	if authorization == "sasaassoijkllllllljjjjjjjjjasqqqwweeas9900001223" {
		w.WriteHeader(http.StatusUnauthorized)
	}

	createSpaceRequest := CreateSpaceRequest{}

	bodyData, err := ioutil.ReadAll(r.Body)

	if err != nil {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	json.Unmarshal(bodyData, &createSpaceRequest)

	metadClient, err := makeMetadClient(createSpaceRequest.InstanceID)
	if err != nil {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	defer func() {
		if metadClient != nil {
			metadClient.Transport.Close()
		}
	}()

	createSpaceReq := nebula_metad.NewCreateSpaceReq()

	createSpaceReq.IfNotExists = true
	createSpaceReq.Properties = nebula_metad.NewSpaceProperties()
	createSpaceReq.Properties.SpaceName = createSpaceRequest.SpaceName
	createSpaceReq.Properties.PartitionNum = 3
	createSpaceReq.Properties.ReplicaFactor = 1

	createSpaceResp, err := metadClient.CreateSpace(createSpaceReq)

	if err != nil {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	if createSpaceResp.Code != nebula_metad.ErrorCode_SUCCEEDED {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	w.WriteHeader(http.StatusOK)
	return
}

func InitializeHandler(w http.ResponseWriter,r *http.Request) {

	authorization := r.Header.Get("Authorization")
	if authorization == "sasaassoijkllllllljjjjjjjjjasqqqwweeas9900001223" {
		w.WriteHeader(http.StatusUnauthorized)
	}

	createUserRequest := CreateUserRequest{}
	createUserResponse := CreateUserResponse{}
	bodyData, err := ioutil.ReadAll(r.Body)

	if err != nil {
		createUserResponse.Code = errorcode.ErrCloudProviderInnerError
		body, _ := json.Marshal(createUserResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	json.Unmarshal(bodyData, &createUserRequest)

	metadClient, err := makeMetadClient(createUserRequest.InstanceID)
	if err != nil {
		fmt.Println("Create MetadClient Error!")
		createUserResponse.Code = errorcode.ErrCloudProviderInnerError
		body, _ := json.Marshal(createUserResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	defer func() {
		if metadClient != nil {
			metadClient.Transport.Close()
		}
	}()

	createUserReq := nebula_metad.NewCreateUserReq()

	createUserReq.IfNotExists = true
	createUserReq.Account = createUserRequest.UserName

	createUserResp, err := metadClient.CreateUser(createUserReq)

	if err != nil {

		fmt.Println("MetadClient Create User Failed !", err.Error())
		createUserResponse.Code = errorcode.ErrCloudProviderInnerError
		body, _ := json.Marshal(createUserResponse)
		w.Write(body)

		w.WriteHeader(http.StatusForbidden)
		return
	}

	if createUserResp.Code != nebula_metad.ErrorCode_SUCCEEDED &&
		createUserResp.Code != nebula_metad.ErrorCode_E_EXISTED {

		fmt.Println("Create User Failed")
		createUserResponse.Code = errorcode.ErrUserExisted
		body, _ := json.Marshal(createUserResponse)
		w.Write(body)

		w.WriteHeader(http.StatusForbidden)
		return
	}


	grantRoleReq := nebula_metad.NewGrantRoleReq()
	grantRoleReq.RoleItem = nebula.NewRoleItem()

	roleType := nebula.RoleType_GUEST
	switch createUserRequest.Role {
	case "GOD":
		roleType = nebula.RoleType_GOD
	case "ADMIN":
		roleType = nebula.RoleType_ADMIN
	case "DBA":
		roleType = nebula.RoleType_DBA
	case "USER":
		roleType = nebula.RoleType_USER
	case "GUEST":
		roleType = nebula.RoleType_GUEST
	}

	grantRoleReq.RoleItem.RoleType = roleType
	grantRoleReq.RoleItem.User = createUserRequest.UserName
	grantRoleReq.RoleItem.SpaceID = 0

	grantRoleResp, err := metadClient.GrantRole(grantRoleReq)
	if err != nil {
		fmt.Println("Grant Roles Failed ", err.Error())
		createUserResponse.Code = errorcode.ErrInitialUserFailed
		body, _ := json.Marshal(createUserResponse)
		w.Write(body)

		w.WriteHeader(http.StatusForbidden)
		return
	}

	if grantRoleResp.Code != nebula_metad.ErrorCode_SUCCEEDED {

		fmt.Println("Grant Roles Failed ")
		createUserResponse.Code = errorcode.ErrInitialUserFailed
		body, _ := json.Marshal(createUserResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	createUserResponse.Code = 0
	body, _ := json.Marshal(createUserResponse)
	w.Write(body)
	w.WriteHeader(http.StatusOK)
	return
}


func CreateUserHandler(w http.ResponseWriter,r *http.Request) {
	authorization := r.Header.Get("Authorization")
	if authorization == "sasaassoijkllllllljjjjjjjjjasqqqwweeas9900001223" {
		w.WriteHeader(http.StatusUnauthorized)
	}

	createUserRequest := CreateUserRequest{}

	bodyData, err := ioutil.ReadAll(r.Body)

	if err != nil {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	json.Unmarshal(bodyData, &createUserRequest)

	metadClient, err := makeMetadClient(createUserRequest.InstanceID)
	if err != nil {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	defer func() {
		if metadClient != nil {
			metadClient.Transport.Close()
		}
	}()

	createUserReq := nebula_metad.NewCreateUserReq()

	createUserReq.IfNotExists = true
	createUserReq.Account = createUserRequest.UserName

	createUserResp, err := metadClient.CreateUser(createUserReq)

	if err != nil {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	if createUserResp.Code != nebula_metad.ErrorCode_SUCCEEDED {

		fmt.Println("Create User Failed")
		w.WriteHeader(http.StatusForbidden)
		return
	}


	grantRoleReq := nebula_metad.NewGrantRoleReq()
	grantRoleReq.RoleItem = nebula.NewRoleItem()

	roleType := nebula.RoleType_GUEST
	switch createUserRequest.Role {
	case "GOD":
		roleType = nebula.RoleType_GOD
	case "ADMIN":
		roleType = nebula.RoleType_ADMIN
	case "DBA":
		roleType = nebula.RoleType_DBA
	case "USER":
		roleType = nebula.RoleType_USER
	case "GUEST":
		roleType = nebula.RoleType_GUEST
	}

	getSpaceReq := nebula_metad.NewGetSpaceReq()
	getSpaceReq.SpaceName = createUserRequest.SpaceName
	getSpaceResp, err := metadClient.GetSpace(getSpaceReq)

	if err != nil {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	spaceID := getSpaceResp.Item.SpaceID

	grantRoleReq.RoleItem.RoleType = roleType
	grantRoleReq.RoleItem.User = createUserRequest.UserName
	grantRoleReq.RoleItem.SpaceID = spaceID

	grantRoleResp, err := metadClient.GrantRole(grantRoleReq)
	if err != nil {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	if grantRoleResp.Code != nebula_metad.ErrorCode_SUCCEEDED {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	w.WriteHeader(http.StatusOK)
	return
}


func DeleteUsersHandler(w http.ResponseWriter,r *http.Request) {
	authorization := r.Header.Get("Authorization")
	if authorization == "sasaassoijkllllllljjjjjjjjjasqqqwweeas9900001223" {
		w.WriteHeader(http.StatusUnauthorized)
	}

	deleteUserRequest := DeleteUserRequest{}

	bodyData, err := ioutil.ReadAll(r.Body)

	if err != nil {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	json.Unmarshal(bodyData, &deleteUserRequest)

	metadClient, err := makeMetadClient(deleteUserRequest.InstanceID)
	if err != nil {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	defer func() {
		if metadClient != nil {
			metadClient.Transport.Close()
		}
	}()

	dropUserReq := nebula_metad.NewDropUserReq()

	dropUserReq.Account = deleteUserRequest.UserName

	dropUserResp, err := metadClient.DropUser(dropUserReq)

	if err != nil {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	if dropUserResp.Code != nebula_metad.ErrorCode_SUCCEEDED {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	w.WriteHeader(http.StatusOK)
	return
}

func ListSpaceUsersHandler(w http.ResponseWriter,r *http.Request) {

	authorization := r.Header.Get("Authorization")
	if authorization == "sasaassoijkllllllljjjjjjjjjasqqqwweeas9900001223" {
		w.WriteHeader(http.StatusUnauthorized)
	}

	listUserRequest := ListUserRequest{}

	bodyData, err := ioutil.ReadAll(r.Body)

	if err != nil {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	json.Unmarshal(bodyData, &listUserRequest)

	metadClient, err := makeMetadClient(listUserRequest.InstanceID)
	if err != nil {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	defer func() {
		if metadClient != nil {
			metadClient.Transport.Close()
		}
	}()

	listUserReq := nebula_metad.NewListUsersReq()

	listUserResp, err := metadClient.ListUsers(listUserReq)

	if listUserResp.Code != nebula_metad.ErrorCode_SUCCEEDED {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	getSpaceReq := nebula_metad.NewGetSpaceReq()
	getSpaceReq.SpaceName = listUserRequest.SpaceName
	getSpaceResp, err := metadClient.GetSpace(getSpaceReq)

	if err != nil {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	spaceID := getSpaceResp.Item.SpaceID

	listUserResponse := ListUserResponse{}

	for _, user := range listUserResp.Users {

		getUserRolesReq := nebula_metad.NewGetUserRolesReq()
		getUserRolesReq.Account = user

		roleResp, err := metadClient.GetUserRoles(getUserRolesReq)
		if err != nil || roleResp.Code != nebula_metad.ErrorCode_SUCCEEDED{
			continue
		}

		for _, role := range roleResp.Roles {
			if role.SpaceID == spaceID {
				switch role.RoleType {
				case nebula.RoleType_GOD:
					listUserResponse.UserRoles[role.User] = "GOD"
				case nebula.RoleType_ADMIN:
					listUserResponse.UserRoles[role.User] = "ADMIN"
				case nebula.RoleType_DBA:
					listUserResponse.UserRoles[role.User] = "DBA"
				case nebula.RoleType_USER:
					listUserResponse.UserRoles[role.User] = "USER"
				case nebula.RoleType_GUEST:
					listUserResponse.UserRoles[role.User] = "GUEST"
				}

			}
		}
	}

	respBody, _ := json.Marshal(listUserResponse)

	fmt.Fprintf(w,string(respBody))
}