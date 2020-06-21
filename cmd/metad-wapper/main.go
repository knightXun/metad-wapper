package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"github.com/vesoft-inc-private/nebula-operator/pkg/errorcode"
	nebula "github.com/vesoft-inc/nebula-go/nebula"
	nebula_metad "github.com/vesoft-inc/nebula-go/nebula/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	timeoutOption := thrift.SocketTimeout(time.Second * 5)
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
	http.HandleFunc("/list/spaces", ListSpaceHandler)
	http.HandleFunc("/create/spaces", CreateSpaceHandler)
	http.HandleFunc("/create/users", CreateUserHandler)
	http.HandleFunc("/delete/users", revokeUsersHandler)
	http.HandleFunc("/initialize", InitializeHandler)
	http.HandleFunc("/list/spaces/users", ListSpaceUsersHandler)
	http.HandleFunc("/list/rootspaces/users", ListRootSpaceUsersHandler)
	http.HandleFunc("/instance/version", InstanceVersion)

	err := http.ListenAndServe("0.0.0.0:8880", nil)

	if err != nil {
		fmt.Println("")
	}
}

type ListSpaceRequest struct {
	InstanceID string
}

type ListSpaceResponse struct {
	InstanceID string
	Spaces     []string
	Code       int
}

type InstanceInfoRequest struct {
	InstanceID string
}

type InstanceInfo struct {
	DiskUsage 	int64   `json:"diskUsage,omitempty"`
	TotalDiskSpace int64	`json:"totalDiskSpace,omitempty"`
	Component string	`json:"component"`
	Version   string	`json:"version"`
	CommitID  string	`json:"commitID"`
	BuildTime string	`json:"buildTime"`
}

type InstanceInfoResponse struct {
	Code       int
	Infos []InstanceInfo `json:"data"`
}

type CreateSpaceRequest struct {
	InstanceID string
	SpaceName  string
}

type CreateUserRequest struct {
	InstanceID string
	UserName   string
	Role       string
	SpaceName  string
	Account    string
}

type CreateUserResponse struct {
	Code int
}

type ListUserRequest struct {
	InstanceID string
	SpaceName  string
}

type ListUserResponse struct {
	UserRoles map[string]string
	Code      int
}

type RevokeUserRequest struct {
	InstanceID string
	UserName   string
	Space      string
	Role       string
	Account    string
}

type RevokeUserResponse struct {
	Code int
}


func GetPVCUsage(instance string) (map[string]int64, error){

	res := map[string]int64{}
	httpPath := "http://prometheus.kube-system:9090/api/v1/query?query=sum(kubelet_volume_stats_capacity_bytes{namespace=\"" + instance + "\"}-kubelet_volume_stats_available_bytes{namespace=\"" + instance + "\"})by(persistentvolumeclaim)"

	httpClient := http.Client{
		Timeout: time.Second * 5,
	}

	resp, err := httpClient.Get(httpPath)

	if err != nil {
		return res, err
	}

	type PrometheusResult struct {
		Metric map[string]string `json:"metric"`
		Value []interface{} `json:"value"`
	}

	type PrometheusData struct {
		Data   []PrometheusResult `json:"result"`
	}

	type PrometheusQueryResult struct {
		Status string         `json:"status"`
		Data PrometheusData `json:"data"`
	}

	prometheusQueryResult := PrometheusQueryResult{}

	bodyData, _ := ioutil.ReadAll(resp.Body)

	json.Unmarshal(bodyData, &prometheusQueryResult)

	if prometheusQueryResult.Status != "success" {
		return res, fmt.Errorf("query prometheus error")
	}

	for _, metric := range prometheusQueryResult.Data.Data {
		if len(metric.Value) != 2 {
			continue
		}

		diskUsageStr := metric.Value[1].(string)

		usage, err := strconv.Atoi(diskUsageStr)

		if err != nil {
			continue
		}

		res[metric.Metric["persistentvolumeclaim"]] = int64(usage)
	}

	return res, nil
}

func InstanceVersion(w http.ResponseWriter, r *http.Request) {
	instanceInfoRequest := InstanceInfoRequest{}
	instanceInfoResponse := InstanceInfoResponse{}
	bodyData, err := ioutil.ReadAll(r.Body)

	if err != nil {
		instanceInfoResponse.Code = errorcode.ErrInvalidRequestBody
		body, _ := json.Marshal(instanceInfoResponse)
		w.Write(body)

		fmt.Println("Invalid InstanceInfoRequest Body")
		w.WriteHeader(http.StatusForbidden)
		return
	}

	json.Unmarshal(bodyData, &instanceInfoRequest)

	fmt.Printf("Get Instance %v Version", instanceInfoRequest.InstanceID)

	//pods, err := client.CoreV1().Pods(instanceInfoRequest.InstanceID).List(metav1.ListOptions{})

	if err != nil {
		instanceInfoResponse.Code = errorcode.ErrInternalError
		body, _ := json.Marshal(instanceInfoResponse)
		w.Write(body)

		log.Printf("List Pods Error: %s", err.Error())

		w.WriteHeader(http.StatusForbidden)
		return
	}

	type NebulaVersionResponse struct {
		Status string `json:"status"`
		BuildTime string `json:"build_time"`
		GitCommitID string `json:"git_info_sha"`
		Version     string `json:"version"`
	}

	diskUsage, err := GetPVCUsage(instanceInfoRequest.InstanceID)

	if err != nil {
		instanceInfoResponse.Code = errorcode.ErrInternalError
		body, _ := json.Marshal(instanceInfoResponse)
		w.Write(body)

		log.Println("List PVC Error: %v", err.Error())
		w.WriteHeader(http.StatusForbidden)
		return
	}


	//for _, pod := range pods.Items {
	//	log.Println("Get %v Version", pod.Name)
	//	if strings.Contains(pod.Name, "graphd") {
	//		graphdHttpPath := "http://" + pod.Status.PodIP + ":13000"
	//		httpClient := http.Client{
	//			Timeout: time.Second * 5,
	//		}
	//
	//		resp, err := httpClient.Get(graphdHttpPath)
	//
	//		if err != nil {
	//			log.Println("Get Graphd Version Failed")
	//			continue
	//		}
	//
	//		bodyData, err := ioutil.ReadAll(resp.Body)
	//
	//		versionResponse := NebulaVersionResponse{}
	//		json.Unmarshal(bodyData, &versionResponse)
	//
	//		instanceInfoResponse.Infos = append(instanceInfoResponse.Infos, InstanceInfo{
	//			Component: "graphd",
	//			Version: versionResponse.Version,
	//			CommitID: versionResponse.GitCommitID,
	//			BuildTime: versionResponse.BuildTime,
	//		})
	//
	//	} else if strings.Contains(pod.Name, "metad") {
	//		metadHttpPath := "http://" + pod.Status.PodIP + ":11000"
	//		httpClient := http.Client{
	//			Timeout: time.Second * 5,
	//		}
	//
	//		resp, err := httpClient.Get(metadHttpPath)
	//
	//		if err != nil {
	//			log.Println("Get Graphd Version Failed")
	//			continue
	//		}
	//
	//		bodyData, err := ioutil.ReadAll(resp.Body)
	//		if err != nil {
	//			log.Println("Get metad Version Failed")
	//			continue
	//		}
	//
	//		versionResponse := NebulaVersionResponse{}
	//		json.Unmarshal(bodyData, &versionResponse)
	//
	//		instanceInfoResponse.Infos = append(instanceInfoResponse.Infos, InstanceInfo{
	//			Component: "metad",
	//			Version: versionResponse.Version,
	//			CommitID: versionResponse.GitCommitID,
	//			BuildTime: versionResponse.BuildTime,
	//			TotalDiskSpace: 20 * 1024 * 1024 * 1024,
	//			DiskUsage: diskUsage["data-metad-0"],
	//		})
	//
	//	} else if strings.Contains(pod.Name, "storaged") {
	//		storagedHttpPath := "http://" + pod.Status.PodIP + ":12000"
	//		httpClient := http.Client{
	//			Timeout: time.Second * 5,
	//		}
	//
	//		resp, err := httpClient.Get(storagedHttpPath)
	//
	//		if err != nil {
	//			log.Println("Get storaged Version Failed")
	//			continue
	//		}
	//
	//		bodyData, err := ioutil.ReadAll(resp.Body)
	//		if err != nil {
	//			continue
	//		}
	//
	//		versionResponse := NebulaVersionResponse{}
	//		json.Unmarshal(bodyData, &versionResponse)
	//
	//		instanceInfoResponse.Infos = append(instanceInfoResponse.Infos, InstanceInfo{
	//			Component: "storaged",
	//			Version: versionResponse.Version,
	//			CommitID: versionResponse.GitCommitID,
	//			BuildTime: versionResponse.BuildTime,
	//			TotalDiskSpace: 20 * 1024 * 1024 * 1024,
	//			DiskUsage: diskUsage["data-storaged-0"],
	//		})
	//	}
	//}

	for pvc,usage := range diskUsage {
		if pvc == "data-storaged-0" {
			instanceInfoResponse.Infos = append(instanceInfoResponse.Infos, InstanceInfo{
				Component: "storaged",
				DiskUsage: usage,
				Version: "v1.0.0",
			})
		}

		if pvc == "data-metad-0" {
			instanceInfoResponse.Infos = append(instanceInfoResponse.Infos, InstanceInfo{
				Component: "metad",
				DiskUsage: usage,
				Version: "v1.0.0",
			})
		}
	}
	respBody, _ := json.Marshal(instanceInfoResponse)

	w.Write(respBody)

	w.WriteHeader(http.StatusOK)
}

func ListSpaceHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("List Spaces")


	listSpaceRequest := ListSpaceRequest{}
	listSpaceResponse := ListSpaceResponse{}

	bodyData, err := ioutil.ReadAll(r.Body)

	if err != nil {
		listSpaceResponse.Code = errorcode.ErrInvalidRequestBody
		body, _ := json.Marshal(listSpaceResponse)
		w.Write(body)

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
		fmt.Println("Create MetadClient for error", listSpaceRequest.InstanceID, err)
		listSpaceResponse.Code = errorcode.ErrInternalError
		body, _ := json.Marshal(listSpaceResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	listSpacesResp, err := metadClient.ListSpaces(&nebula_metad.ListSpacesReq{})

	if err != nil {
		fmt.Println("List Spaces for error: ", listSpaceRequest.InstanceID, err)
		listSpaceResponse.Code = errorcode.ErrInternalError
		body, _ := json.Marshal(listSpaceResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	if listSpacesResp.Code != nebula_metad.ErrorCode_SUCCEEDED {
		fmt.Println("List Spaces for error code: ", listSpaceRequest.InstanceID, listSpacesResp.Code)
		listSpaceResponse.Code = errorcode.ErrInternalError
		body, _ := json.Marshal(listSpaceResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	ids := listSpacesResp.GetSpaces()

	if ids == nil {
		fmt.Println("No Spaces")
		listSpaceResponse.Code = errorcode.ErrInternalError
		body, _ := json.Marshal(listSpaceResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	fmt.Println("Parse SpaceResponse")

	listSpaceResponse.Spaces = []string{}
	for _, id := range ids {
		listSpaceResponse.Spaces = append(listSpaceResponse.Spaces, id.Name)
	}

	if listSpaceResponse.Spaces == nil && len(listSpaceResponse.Spaces) == 0 {
		listSpaceResponse.Spaces = make([]string,0)
	}

	listSpaceResponse.InstanceID = listSpaceRequest.InstanceID
	listSpaceResponse.Code = 0

	respBody, _ := json.Marshal(listSpaceResponse)

	w.Write(respBody)

	w.WriteHeader(http.StatusOK)
}

func CreateSpaceHandler(w http.ResponseWriter, r *http.Request) {

	fmt.Println("Begin Create Space")

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

	createSpaceReq.Properties = nebula_metad.NewSpaceProperties()
	createSpaceReq.Properties.SpaceName = createSpaceRequest.SpaceName
	createSpaceReq.Properties.PartitionNum = 3
	createSpaceReq.Properties.ReplicaFactor = 1

	createSpaceResp, err := metadClient.CreateSpace(createSpaceReq)

	if err != nil {
		fmt.Println("create space " + createSpaceRequest.SpaceName + " error: " + err.Error())
		w.WriteHeader(http.StatusForbidden)
		return
	}

	if createSpaceResp.Code != nebula_metad.ErrorCode_SUCCEEDED {
		fmt.Println("create space failed, ErrorCode is ", createSpaceResp.Code)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	fmt.Println("Create Space Done")
	w.WriteHeader(http.StatusOK)
	return
}

func InitializeHandler(w http.ResponseWriter, r *http.Request) {
	createUserRequest := CreateUserRequest{}
	createUserResponse := CreateUserResponse{}
	bodyData, err := ioutil.ReadAll(r.Body)

	if err != nil {
		createUserResponse.Code = errorcode.ErrInternalError
		body, _ := json.Marshal(createUserResponse)
		w.WriteHeader(http.StatusForbidden)
		w.Write(body)
		return
	}

	json.Unmarshal(bodyData, &createUserRequest)

	metadClient, err := makeMetadClient(createUserRequest.InstanceID)
	if err != nil {
		fmt.Println("Create MetadClient Error!")
		createUserResponse.Code = errorcode.ErrInternalError
		body, _ := json.Marshal(createUserResponse)
		w.WriteHeader(http.StatusForbidden)
		w.Write(body)
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
		createUserResponse.Code = errorcode.ErrInternalError
		body, _ := json.Marshal(createUserResponse)
		w.WriteHeader(http.StatusForbidden)
		w.Write(body)
		return
	}

	if createUserResp.Code != nebula_metad.ErrorCode_SUCCEEDED &&
		createUserResp.Code != nebula_metad.ErrorCode_E_EXISTED {

		fmt.Println("Create User Failed")
		createUserResponse.Code = errorcode.ErrUserExisted
		body, _ := json.Marshal(createUserResponse)
		w.WriteHeader(http.StatusForbidden)
		w.Write(body)
		return
	}

	if createUserResp.Code == nebula_metad.ErrorCode_E_EXISTED {
		createUserResponse.Code = 0
		body, _ := json.Marshal(createUserResponse)
		w.WriteHeader(http.StatusForbidden)
		w.Write(body)
		return
	}

	fmt.Println("Create USER " + createUserRequest.UserName + " Success")

	grantRoleReq := nebula_metad.NewGrantRoleReq()
	grantRoleReq.RoleItem = nebula.NewRoleItem()

	grantRoleReq.RoleItem.RoleType = nebula.RoleType_GOD
	grantRoleReq.RoleItem.User = createUserRequest.UserName
	grantRoleReq.RoleItem.SpaceID = 0

	fmt.Println("Begin Grant " + createUserRequest.UserName + " to GOD")
	grantRoleResp, err := metadClient.GrantRole(grantRoleReq)
	if err != nil {
		fmt.Println("Grant Roles Failed: " + err.Error())
		createUserResponse.Code = errorcode.ErrInitialUserFailed
		body, _ := json.Marshal(createUserResponse)
		w.WriteHeader(http.StatusForbidden)
		w.Write(body)
		return
	}

	if grantRoleResp.Code != nebula_metad.ErrorCode_SUCCEEDED {
		fmt.Println("Grant Roles ErrorCode is : ", grantRoleResp.Code)
		createUserResponse.Code = errorcode.ErrInitialUserFailed
		body, _ := json.Marshal(createUserResponse)
		w.WriteHeader(http.StatusForbidden)
		w.Write(body)
		return
	}

	createUserResponse.Code = 0
	body, _ := json.Marshal(createUserResponse)
	w.WriteHeader(http.StatusOK)
	w.Write(body)
	fmt.Println("Create GOD User" + createUserRequest.UserName + " Success!")
	return
}

func GetUserRoles(user , spaceName string, metadClient *nebula_metad.MetaServiceClient ) (nebula.RoleType, error) {
	spaceID, err := getSpaceID(spaceName, metadClient)
	if err != nil {
		fmt.Println("List User Failed")
		return -1, fmt.Errorf("Inner Error")
	}

	getUserRolesReq := nebula_metad.NewGetUserRolesReq()
	getUserRolesReq.Account = user

	roleResp, err := metadClient.GetUserRoles(getUserRolesReq)
	if err != nil {
		fmt.Println("Get User " + user + " error " + err.Error())
		return -1, fmt.Errorf("Inner Error")
	}

	for _, role := range roleResp.Roles {
		if role.SpaceID == spaceID {
			fmt.Println("Account Role is: ", role.RoleType)
			return role.RoleType, nil
		}
	}

	return nebula.RoleType_GUEST, nil
}

func CreateUserHandler(w http.ResponseWriter, r *http.Request) {
	createUserRequest := CreateUserRequest{}
	createUserResponse := CreateUserResponse{}
	bodyData, err := ioutil.ReadAll(r.Body)

	if err != nil {
		fmt.Println("Invalid Request Body")
		createUserResponse.Code = errorcode.ErrInvalidRequestBody
		body, _ := json.Marshal(createUserResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	json.Unmarshal(bodyData, &createUserRequest)

	fmt.Println("CreateUserRequest", createUserRequest)

	metadClient, err := makeMetadClient(createUserRequest.InstanceID)
	if err != nil {
		fmt.Println("Create Metad Client Failed ", err.Error())
		createUserResponse.Code = errorcode.ErrInternalError
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

	revokerRole, err := GetUserRoles(createUserRequest.Account, createUserRequest.SpaceName, metadClient)
	if err != nil {
		fmt.Println("Create User Failed ", err.Error())
		createUserResponse.Code = errorcode.ErrGrantRoleFailed
		body, _ := json.Marshal(createUserResponse)
		w.Write(body)

		w.WriteHeader(http.StatusForbidden)
		return
	}

	if revokerRole > roleType {
		fmt.Println("Create User Failed: FatherAccount Role larger then Role")
		createUserResponse.Code = errorcode.ErrGrantRoleFailed
		body, _ := json.Marshal(createUserResponse)
		w.Write(body)

		w.WriteHeader(http.StatusForbidden)
		return
	}

	createUserReq := nebula_metad.NewCreateUserReq()

	createUserReq.IfNotExists = true
	createUserReq.Account = createUserRequest.UserName

	createUserResp, err := metadClient.CreateUser(createUserReq)

	if err != nil {
		fmt.Println("Create User Failed ", err.Error())
		createUserResponse.Code = errorcode.ErrInternalError
		body, _ := json.Marshal(createUserResponse)
		w.Write(body)

		w.WriteHeader(http.StatusForbidden)
		return
	}

	if createUserResp.Code != nebula_metad.ErrorCode_SUCCEEDED {
		if createUserResp.Code == nebula_metad.ErrorCode_E_EXISTED {

		} else {
			createUserResponse.Code = errorcode.ErrInternalError
			body, _ := json.Marshal(createUserResponse)
			w.Write(body)
			fmt.Println("Create User Failed")
			w.WriteHeader(http.StatusForbidden)
			return
		}
	}

	grantRoleReq := nebula_metad.NewGrantRoleReq()
	grantRoleReq.RoleItem = nebula.NewRoleItem()

	getSpaceReq := nebula_metad.NewGetSpaceReq()
	getSpaceReq.SpaceName = createUserRequest.SpaceName
	fmt.Println("Get Space, Name is :  " + createUserRequest.SpaceName)
	getSpaceResp, err := metadClient.GetSpace(getSpaceReq)

	if err != nil {
		fmt.Println("Get Space Failed ", err.Error())

		createUserResponse.Code = errorcode.ErrInternalError
		body, _ := json.Marshal(createUserResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	if getSpaceResp.Code != nebula_metad.ErrorCode_SUCCEEDED {
		if getSpaceResp.Code == nebula_metad.ErrorCode_E_NOT_FOUND {
			createUserResponse.Code = errorcode.ErrSpaceNotFound
			body, _ := json.Marshal(createUserResponse)
			w.Write(body)
			fmt.Println("Get Space Failed ", getSpaceResp.Code)
			w.WriteHeader(http.StatusForbidden)
			return
		} else {
			createUserResponse.Code = errorcode.ErrInternalError
			body, _ := json.Marshal(createUserResponse)
			w.Write(body)
			fmt.Println("Get Space Error ", getSpaceResp.Code)
			w.WriteHeader(http.StatusForbidden)
			return
		}
	}

	spaceID := getSpaceResp.Item.SpaceID

	grantRoleReq.RoleItem.RoleType = roleType
	grantRoleReq.RoleItem.User = createUserRequest.UserName
	grantRoleReq.RoleItem.SpaceID = spaceID

	fmt.Println("Grant User SpaceID", spaceID, "UserName", createUserRequest.UserName, "RoleType", roleType)
	grantRoleResp, err := metadClient.GrantRole(grantRoleReq)
	if err != nil {
		fmt.Println("Grant Role Failed ", err.Error())
		createUserResponse.Code = errorcode.ErrInternalError
		body, _ := json.Marshal(createUserResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	if grantRoleResp.Code != nebula_metad.ErrorCode_SUCCEEDED {
		createUserResponse.Code = errorcode.ErrInternalError
		body, _ := json.Marshal(createUserResponse)
		w.Write(body)
		fmt.Println("Grant User Error ", getSpaceResp.Code)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	createUserResponse.Code = 0
	body, _ := json.Marshal(createUserResponse)
	w.Write(body)
	w.WriteHeader(http.StatusOK)
	return
}

func revokeUsersHandler(w http.ResponseWriter, r *http.Request) {
	deleteUserRequest := RevokeUserRequest{}
	deleteUserResponse := RevokeUserResponse{}

	bodyData, err := ioutil.ReadAll(r.Body)

	if err != nil {
		deleteUserResponse.Code = errorcode.ErrInvalidRequestBody
		body, _ := json.Marshal(deleteUserResponse)
		w.Write(body)

		w.WriteHeader(http.StatusForbidden)
		return
	}

	json.Unmarshal(bodyData, &deleteUserRequest)

	metadClient, err := makeMetadClient(deleteUserRequest.InstanceID)
	if err != nil {

		fmt.Println("Create Metad Client Error ", err.Error())
		deleteUserResponse.Code = errorcode.ErrInternalError
		body, _ := json.Marshal(deleteUserResponse)
		w.Write(body)

		w.WriteHeader(http.StatusForbidden)
		return
	}

	defer func() {
		if metadClient != nil {
			metadClient.Transport.Close()
		}
	}()

	roleType := nebula.RoleType_GUEST
	switch deleteUserRequest.Role {
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

	revokerRole, err := GetUserRoles(deleteUserRequest.Account, deleteUserRequest.Space, metadClient)
	if err != nil {
		fmt.Println("Delete User Failed ", err.Error())
		deleteUserResponse.Code = errorcode.ErrGrantRoleFailed
		body, _ := json.Marshal(deleteUserResponse)
		w.Write(body)

		w.WriteHeader(http.StatusForbidden)
		return
	}

	if revokerRole > roleType {
		fmt.Println("Delete User Failed")
		deleteUserResponse.Code = errorcode.ErrGrantRoleFailed
		body, _ := json.Marshal(deleteUserResponse)
		w.Write(body)

		w.WriteHeader(http.StatusForbidden)
		return
	}


	spaceID, err := getSpaceID(deleteUserRequest.Space, metadClient)

	if err != nil {
		fmt.Println("Get SpaceID Failed ", err.Error())
		deleteUserResponse.Code = errorcode.ErrInternalError
		body, _ := json.Marshal(deleteUserResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	dropUserReq := nebula_metad.NewRevokeRoleReq()
	dropUserReq.RoleItem = new(nebula.RoleItem)
	dropUserReq.RoleItem.User = deleteUserRequest.UserName
	dropUserReq.RoleItem.SpaceID = spaceID
	dropUserReq.RoleItem.RoleType = roleType

	revokeRoleResp, err := metadClient.RevokeRole(dropUserReq)

	if err != nil {

		fmt.Println("Revoke User Failed ", err.Error())
		deleteUserResponse.Code = errorcode.ErrInternalError
		body, _ := json.Marshal(deleteUserResponse)
		w.Write(body)

		w.WriteHeader(http.StatusForbidden)
		return
	}

	if revokeRoleResp.Code != nebula_metad.ErrorCode_SUCCEEDED {
		fmt.Println("Revoke User Failed ")
		deleteUserResponse.Code = errorcode.ErrInternalError
		body, _ := json.Marshal(deleteUserResponse)
		w.Write(body)

		w.WriteHeader(http.StatusForbidden)
		return
	}

	deleteUserResponse.Code = 0
	body, _ := json.Marshal(deleteUserResponse)
	w.Write(body)

	w.WriteHeader(http.StatusOK)
	return
}

func ListSpaceUsersHandler(w http.ResponseWriter, r *http.Request) {
	listUserRequest := ListUserRequest{}
	listUserResponse := ListUserResponse{}

	bodyData, err := ioutil.ReadAll(r.Body)

	if err != nil {
		fmt.Println("Invalid Request Body")
		listUserResponse.Code = errorcode.ErrInvalidRequestBody
		body, _ := json.Marshal(listUserResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	json.Unmarshal(bodyData, &listUserRequest)

	metadClient, err := makeMetadClient(listUserRequest.InstanceID)
	if err != nil {
		fmt.Println("Create MetadClient Error ", err.Error())
		listUserResponse.Code = errorcode.ErrInternalError
		body, _ := json.Marshal(listUserResponse)
		w.Write(body)

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
		fmt.Println("List User Failed")
		listUserResponse.Code = errorcode.ErrInternalError
		body, _ := json.Marshal(listUserResponse)
		w.Write(body)

		w.WriteHeader(http.StatusForbidden)
		return
	}

	getSpaceReq := nebula_metad.NewGetSpaceReq()
	fmt.Println("SpaceName is ", listUserRequest.SpaceName)
	getSpaceReq.SpaceName = listUserRequest.SpaceName
	getSpaceResp, err := metadClient.GetSpace(getSpaceReq)

	if err != nil {
		fmt.Println("Get Space Failed ", err.Error())
		listUserResponse.Code = errorcode.ErrInternalError
		body, _ := json.Marshal(listUserResponse)
		w.Write(body)

		w.WriteHeader(http.StatusForbidden)
		return
	}

	if getSpaceResp.Code != nebula_metad.ErrorCode_SUCCEEDED {
		fmt.Println("Get Space Failed, ErrorCode is ", getSpaceResp.Code)
		listUserResponse.Code = errorcode.ErrNotFound
		body, _ := json.Marshal(listUserResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	spaceID := getSpaceResp.Item.SpaceID
	fmt.Println("SpaceID is ", getSpaceResp.Item.SpaceID)

	listUserResponse.UserRoles = make(map[string]string)

	for user, userid := range listUserResp.Users {
		fmt.Println("Get Roles of ", userid, user)

		if user == "root" {
			continue
		}

		getUserRolesReq := nebula_metad.NewGetUserRolesReq()
		getUserRolesReq.Account = user

		roleResp, err := metadClient.GetUserRoles(getUserRolesReq)
		if err != nil {
			fmt.Println("Get User " + user + " error " + err.Error())
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

	listUserResponse.Code = 0
	respBody, _ := json.Marshal(listUserResponse)

	fmt.Fprintf(w, string(respBody))
}

func ListRootSpaceUsersHandler(w http.ResponseWriter, r *http.Request) {
	listUserRequest := ListUserRequest{}
	listUserResponse := ListUserResponse{}

	bodyData, err := ioutil.ReadAll(r.Body)

	if err != nil {
		fmt.Println("Invalid Request Body")
		listUserResponse.Code = errorcode.ErrInvalidRequestBody
		body, _ := json.Marshal(listUserResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	json.Unmarshal(bodyData, &listUserRequest)

	metadClient, err := makeMetadClient(listUserRequest.InstanceID)
	if err != nil {
		fmt.Println("Create MetadClient Error ", err.Error())
		listUserResponse.Code = errorcode.ErrInternalError
		body, _ := json.Marshal(listUserResponse)
		w.Write(body)

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
		fmt.Println("List User Failed")
		listUserResponse.Code = errorcode.ErrInternalError
		body, _ := json.Marshal(listUserResponse)
		w.Write(body)

		w.WriteHeader(http.StatusForbidden)
		return
	}

	spaceID := nebula.GraphSpaceID(0)

	listUserResponse.UserRoles = make(map[string]string)

	for user, userid := range listUserResp.Users {
		fmt.Println("Get Roles of ", userid, user)

		getUserRolesReq := nebula_metad.NewGetUserRolesReq()
		getUserRolesReq.Account = user

		roleResp, err := metadClient.GetUserRoles(getUserRolesReq)
		if err != nil {
			fmt.Println("Get User " + user + " error " + err.Error())
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

	listUserResponse.Code = 0
	respBody, _ := json.Marshal(listUserResponse)

	fmt.Fprintf(w, string(respBody))
}

func getSpaceID(spaceName string, metadClient *nebula_metad.MetaServiceClient) (nebula.GraphSpaceID, error) {
	getSpaceReq := nebula_metad.NewGetSpaceReq()
	fmt.Println("SpaceName is ", spaceName)
	getSpaceReq.SpaceName = spaceName
	getSpaceResp, err := metadClient.GetSpace(getSpaceReq)

	if err != nil {
		return 0, err
	}

	if getSpaceResp.Code != nebula_metad.ErrorCode_SUCCEEDED {
		return -1, fmt.Errorf("Not Found Spaces")
	}

	spaceID := getSpaceResp.Item.SpaceID
	fmt.Println("SpaceID is ", getSpaceResp.Item.SpaceID)
	return spaceID, nil
}
