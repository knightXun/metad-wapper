package main

import (
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
	transport, err := thrift.NewSocket(timeoutOption, addressOption)
	protocol := thrift.NewBinaryProtocolFactoryDefault()
	if err != nil {
		return nil, err
	}

	metadClient := nebula_metad.NewMetaServiceClientFactory(transport, protocol)

	return metadClient, nil
}

func main() {
	http.HandleFunc("/list/spaces",ListSpaceHandler)
	http.HandleFunc("/create/users",CreateSpaceHandler)
	http.HandleFunc("/delete/users",DeleteUsersHandler)
	http.HandleFunc("/list/spaces/users",ListSpaceUsersHandler)

	err := http.ListenAndServe("0.0.0.0:8880",nil)

	if err != nil {
		fmt.Println("")
	}
}

type SpaceRequest struct {
	InstanceID string
}

type CreateSpaceRequest struct {
	InstanceID string
	SpaceName string
}

type SpaceResponse struct {
	InstanceID string
	Spaces []string
}

type ListUserRequest struct {
	InstanceID string
	UserName   string
	SpaceName  string
}

type ListUserResponse struct {
	UserRoles map[string]string
}

type DeleteUserRequest struct {
	InstanceID string
	UserName   string
	SpaceName  string
}

func ListSpaceHandler(w http.ResponseWriter,r *http.Request) {
	fmt.Println("List Spaces")

	spaceRequest := SpaceRequest{}

	bodyData, err := ioutil.ReadAll(r.Body)

	if err != nil {
		fmt.Println("Invalid SpaceRequest Body")
		w.WriteHeader(http.StatusForbidden)
		return
	}
	json.Unmarshal(bodyData, &spaceRequest)

	metadClient, err := makeMetadClient(spaceRequest.InstanceID)

	if err != nil {
		fmt.Println("Create MetadClient for %v error: %v", spaceRequest.InstanceID, err)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	listSpacesResp, err := metadClient.ListSpaces(&nebula_metad.ListSpacesReq{})

	if err != nil {
		fmt.Println("List Spaces for %v error: %v", spaceRequest.InstanceID, err)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	if listSpacesResp.Code != nebula_metad.ErrorCode_SUCCEEDED {
		fmt.Println("List Spaces for %v error code: %v", spaceRequest.InstanceID, listSpacesResp.Code)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	ids := listSpacesResp.GetSpaces()

	if ids == nil {
		fmt.Println("No Spaces")
		w.WriteHeader(http.StatusForbidden)
		return
	}

	fmt.Println("Parse SpaceResponse")
	spaceResponse := SpaceResponse{}

	for _, id := range ids {
		spaceResponse.Spaces = append(spaceResponse.Spaces, id.Name)
	}

	spaceResponse.InstanceID = spaceRequest.InstanceID

	respBody, _ := json.Marshal(spaceResponse)

	w.Write(respBody)
	w.WriteHeader(http.StatusOK)
}

func CreateSpaceHandler(w http.ResponseWriter,r *http.Request) {

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

func DeleteUsersHandler(w http.ResponseWriter,r *http.Request) {
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