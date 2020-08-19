package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	corev1 "k8s.io/api/core/v1"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	nebula "github.com/vesoft-inc/nebula-go/nebula"
	nebula_metad "github.com/vesoft-inc/nebula-go/nebula/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	metricsv1beta1api "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metricsclientset "k8s.io/metrics/pkg/client/clientset/versioned"
)


const (
	ErrNotFound                = 40001
	ErrIllegalMemory           = 40002
	ErrIllegalCPU              = 40003
	ErrNoResource              = 40004
	ErrNoMoney                 = 40005
	ErrS3NoStorage             = 40006
	ErrNoInstance              = 40007
	ErrEmptyInstanceID         = 40008
	ErrInvalidRequestBody      = 40009
	ErrEmptySpaceName          = 40010
	ErrCloudProviderInnerError = 40011
	ErrUserExisted             = 40012
	ErrGrantRoleFailed         = 40013
	ErrInitialUserFailed       = 40014
	ErrInternalError           = 40015
	ErrSpaceNotFound           = 40016
)

var client *kubernetes.Clientset
var metricsClient *metricsclientset.Clientset

func init() {
	client = makeKubeClient()
	metricsClient = makeMetircClient()
}

func makeMetircClient() *metricsclientset.Clientset {
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("Can't Create K8s Client: %v", err)
	}

	metricsClient, err := metricsclientset.NewForConfig(config)

	if err != nil {
		log.Fatalf("Can't Create K8s Client: %v", err)
		return nil
	}
	return metricsClient
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
	metadSvc, err := client.CoreV1().Services(ns).Get(context.Background(),"nebula-metad", metav1.GetOptions{})

	if err != nil {
		return nil, err
	}

	metadSvcIp := metadSvc.Spec.ClusterIP

	timeoutOption := thrift.SocketTimeout(time.Second * 5)
	addressOption := thrift.SocketAddr(metadSvcIp + ":44500")

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
	http.HandleFunc("/list/users", ListUsersHandler)
	http.HandleFunc("/create/spaces", CreateSpaceHandler)
	http.HandleFunc("/create/users", CreateUserHandler)
	http.HandleFunc("/clusterCost", ClusterCosts)
	http.HandleFunc("/changeGod", changeGod)
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
	UserName   string
}

type ListSpaceResponse struct {
	InstanceID string
	Spaces     []string
	Code       int
}

type ListUsersRequest struct {
	InstanceID string
}

type ListUsersResponse struct {
	Users      []string
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

type TransferGodUserRequest struct {
	InstanceID string
	UserName   string
	OldName    string
}

type TransferGodUserResponse struct {
	Code int
}

type ListUserRequest struct {
	InstanceID string
	SpaceName  string
	Operator   string
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

type Machine struct {
	Duration string `json:"duration,omitempty"`
	Cpu int64 	`json:"cpu,omitempty"`
	Memory int64 `json:"memory,omitempty"`
}

type Disk struct {
	Duration 	string `json:"duration,omitempty"`
	Size 		int64 	`json:"size,omitempty"`
	Usage       int64   `json:"usage,omitempty"`
	DiskName    string   `json:"diskName,omitempty"`
}

type LoadBalacer struct {
	Duration 	string `json:"duration,omitempty"`
	Band 		int64 	`json:"band,omitempty"`
}

type Instance struct {
	InstanceName string `json:"instanceName,omitempty"`
	Cpu 		 int64 `json:"cpu,omitempty"`
	CpuUsage     int64 `json:"cpuUsage,omitempty"`
	MemoryUsage     int64 `json:"memoryUsage,omitempty"`
	Memory 		 int64 `json:"memory,omitempty"`
	Disks  		 []Disk `json:"disks,omitempty"`
}

type ClusterCost struct {
	ClusterName 	string  `json:"clusterName,omitempty"`
	Machines 		[]Machine `json:"machines,omitempty"`
	Disks    		[]Disk		`json:"disks,omitempty"`
	LoadBalacer 	[]LoadBalacer `json:"loadBalacer,omitempty"`
	Instances  		[]Instance  `json:"instances,omitempty"`
}

type ClusterCostResponse struct {
	Code 		int 		`json:"code,omitempty"`
	ClusterCost ClusterCost `json:"clusterCost,omitempty"`
}

func GetPodMertris(instance string) (*metricsv1beta1api.PodMetricsList, error) {
	m, err := metricsClient.MetricsV1beta1().PodMetricses(instance).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return m, nil
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
		instanceInfoResponse.Code = ErrInvalidRequestBody
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
		instanceInfoResponse.Code = ErrInternalError
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
		instanceInfoResponse.Code = ErrInternalError
		body, _ := json.Marshal(instanceInfoResponse)
		w.Write(body)

		log.Println("List PVC Error: %v", err.Error())
		w.WriteHeader(http.StatusForbidden)
		return
	}

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

func ClusterCosts(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Get Cluster Costs")

	clusterCostResponse := ClusterCostResponse{}

	nss, err := client.CoreV1().Namespaces().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		fmt.Println("Inner Error: %v", err)
		clusterCostResponse.Code = ErrInternalError
		body, _ := json.Marshal(clusterCostResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	for _, ns := range nss.Items {
		pvcs, err := client.CoreV1().PersistentVolumeClaims(ns.Name).List(context.Background(), metav1.ListOptions{})
		if err != nil {
			fmt.Println("Inner Error: %v", err)
			clusterCostResponse.Code = ErrInternalError
			body, _ := json.Marshal(clusterCostResponse)
			w.Write(body)
			w.WriteHeader(http.StatusForbidden)
			return
		}

		pvcUsage, err := GetPVCUsage(ns.Name)

		if strings.Contains(ns.Name, "nebula") {
			if ns.DeletionTimestamp != nil {
				continue
			}

			if err != nil {
				fmt.Println("Inner Error: %v", err)
				clusterCostResponse.Code = ErrInternalError
				body, _ := json.Marshal(clusterCostResponse)
				w.Write(body)
				w.WriteHeader(http.StatusForbidden)
				return
			}

			podMetrics, err := GetPodMertris(ns.Name)
			if err != nil {
				fmt.Println("Inner Error: %v", err)
				clusterCostResponse.Code = ErrInternalError
				body, _ := json.Marshal(clusterCostResponse)
				w.Write(body)
				w.WriteHeader(http.StatusForbidden)
				return
			}

			cpuUsage := int64(0)
			memUsage := int64(0)

			for _, metric := range podMetrics.Items {
				for _, container := range metric.Containers {
					cpuUsage = cpuUsage + int64(container.Usage.Cpu().Size() )
					//fmt.Printf("Memory Usage %v: %v",container.Name, container.Usage.Memory().Value()/(1024*1024))
					memUsage = memUsage + int64(container.Usage.Memory().Value()/(1024*1024))
				}
			}

			instance := Instance{
				InstanceName: ns.Name,
				Cpu: 1000,
				Memory: 1024,
				CpuUsage: cpuUsage,
				MemoryUsage: memUsage,
			}

			for _, pvc := range pvcs.Items {
				size := pvc.Status.Capacity.Storage().Value()
				duration := time.Now().Sub(pvc.CreationTimestamp.Time).String()
				name := ""
				if strings.Contains(pvc.Name, "metad") {
					name = "metad"
				} else {
					name = "storaged"
				}
				instance.Disks = append(instance.Disks, Disk{
					Duration: duration,
					Size: size,
					Usage: pvcUsage[pvc.Name],
					DiskName: name,
				})

			}

			clusterCostResponse.ClusterCost.Instances = append(clusterCostResponse.ClusterCost.Instances, instance)
		}
		//else {
		//	for _, pvc := range pvcs.Items {
		//		size := pvc.Status.Capacity.Storage().Value()
		//		duration := time.Now().Sub(pvc.CreationTimestamp.Time).String()
		//
		//		clusterCostResponse.ClusterCost.Disks = append(clusterCostResponse.ClusterCost.Disks, Disk{
		//			Duration: duration,
		//			Size: size,
		//			Usage: pvcUsage[pvc.Name],
		//		})
		//	}
		//}

		loadBalancers, err := client.CoreV1().Services(ns.Name).List(context.Background(), metav1.ListOptions{})
		if err != nil {
			fmt.Println("List LoadBalancer In %s Failed: %v", ns.Name, err )
			continue
		}
		for _, loadBalancer := range loadBalancers.Items {
			duration := time.Now().Sub(loadBalancer.CreationTimestamp.Time).String()
			if loadBalancer.Spec.Type == corev1.ServiceTypeLoadBalancer {
				clusterCostResponse.ClusterCost.LoadBalacer = append(clusterCostResponse.ClusterCost.LoadBalacer, LoadBalacer{
					Duration: duration,
					Band: 10,
				})
			}
		}
	}

	nodes, err := client.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})

	if err != nil {
		fmt.Println("Inner Error: %v", err)
		clusterCostResponse.Code = ErrInternalError
		body, _ := json.Marshal(clusterCostResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	for _, node := range nodes.Items {
		cpu := node.Status.Capacity.Cpu().Value()
		memory := node.Status.Capacity.Memory().Value()
		duration := time.Now().Sub(node.CreationTimestamp.Time).String()
		clusterCostResponse.ClusterCost.Machines = append(clusterCostResponse.ClusterCost.Machines, Machine{
			Duration: duration,
			Cpu: cpu,
			Memory: memory/(1024*1024),
		})
	}

	respBody, _ := json.Marshal(clusterCostResponse)

	fmt.Println("Get Cluster Costs Done")

	w.Write(respBody)

	w.WriteHeader(http.StatusOK)
}


func ListSpaceHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("List Spaces")

	listSpaceRequest := ListSpaceRequest{}
	listSpaceResponse := ListSpaceResponse{}

	bodyData, err := ioutil.ReadAll(r.Body)

	if err != nil {
		listSpaceResponse.Code = ErrInvalidRequestBody
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
		listSpaceResponse.Code = ErrInternalError
		body, _ := json.Marshal(listSpaceResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	listSpacesResp, err := metadClient.ListSpaces(&nebula_metad.ListSpacesReq{})

	if err != nil {
		fmt.Println("List Spaces for error: ", listSpaceRequest.InstanceID, err)
		listSpaceResponse.Code = ErrInternalError
		body, _ := json.Marshal(listSpaceResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	if listSpacesResp.Code != nebula_metad.ErrorCode_SUCCEEDED {
		fmt.Println("List Spaces for error code: ", listSpaceRequest.InstanceID, listSpacesResp.Code)
		listSpaceResponse.Code = ErrInternalError
		body, _ := json.Marshal(listSpaceResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	ids := listSpacesResp.GetSpaces()

	if ids == nil {
		fmt.Println("No Spaces")
		listSpaceResponse.Code = ErrInternalError
		body, _ := json.Marshal(listSpaceResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	fmt.Println("Parse SpaceResponse")

	listSpaceResponse.Spaces = []string{}
	for _, id := range ids {
		if isUserInSpace(id.Name, listSpaceRequest.UserName, metadClient) {
			listSpaceResponse.Spaces = append(listSpaceResponse.Spaces, id.Name)
		}
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

func ListUsersHandler(w http.ResponseWriter, r *http.Request) {
	listUsersRequest := ListUsersRequest{}
	listUsersResponse := ListUsersResponse{}

	bodyData, err := ioutil.ReadAll(r.Body)

	if err != nil {
		listUsersResponse.Code = ErrInvalidRequestBody
		body, _ := json.Marshal(listUsersResponse)
		w.Write(body)

		fmt.Println("Invalid SpaceRequest Body")
		w.WriteHeader(http.StatusForbidden)
		return
	}
	json.Unmarshal(bodyData, &listUsersRequest)


	users, err := ListUsers(listUsersRequest.InstanceID)
	if err != nil {
		log.Println("Get User from %s Error: %v", listUsersRequest.InstanceID, err)
		listUsersResponse.Code = ErrInternalError
		body, _ := json.Marshal(listUsersResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	spaces, err := ListSpaces(listUsersRequest.InstanceID)
	if err != nil {
		log.Println("Get Spaces from %s Error: %v", listUsersRequest.InstanceID, err)
		listUsersResponse.Code = ErrInternalError
		body, _ := json.Marshal(listUsersResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	for _, user := range users {
		for _, space := range spaces {
			if isUserInSpace(space, user, listUsersRequest.InstanceID) {
				listUsersResponse.Users = append(listUsersResponse.Users, user)
			}
		}
	}
	respBody, _ := json.Marshal(listUsersResponse)

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

func changeGod(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Handle Change GOD Request")

	transferGodUserRequest := TransferGodUserRequest{}
	transferGodUserResponse := TransferGodUserResponse{}
	bodyData, err := ioutil.ReadAll(r.Body)

	if err != nil {
		fmt.Println("Invalid Request Body")
		transferGodUserResponse.Code = ErrInternalError
		body, _ := json.Marshal(transferGodUserResponse)
		w.WriteHeader(http.StatusForbidden)
		w.Write(body)
		return
	}

	json.Unmarshal(bodyData, &transferGodUserRequest)

	metadClient, err := makeMetadClient(transferGodUserRequest.InstanceID)
	if err != nil {
		fmt.Println("Create MetadClient Error!")
		transferGodUserResponse.Code = ErrInternalError
		body, _ := json.Marshal(transferGodUserResponse)
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

	createUserReq.Account = transferGodUserRequest.UserName

	createUserResp, err := metadClient.CreateUser(createUserReq)

	if err != nil {
		fmt.Println("MetadClient Create User Failed !", err.Error())
		transferGodUserResponse.Code = ErrInternalError
		body, _ := json.Marshal(transferGodUserResponse)
		w.WriteHeader(http.StatusForbidden)
		w.Write(body)
		return
	}

	if createUserResp.Code != nebula_metad.ErrorCode_SUCCEEDED &&
		createUserResp.Code != nebula_metad.ErrorCode_E_EXISTED {
		fmt.Println("Create User Failed")
		transferGodUserResponse.Code = ErrUserExisted
		body, _ := json.Marshal(transferGodUserResponse)
		w.WriteHeader(http.StatusForbidden)
		w.Write(body)
		return
	}

	//if createUserResp.Code == nebula_metad.ErrorCode_E_EXISTED {
	//	transferGodUserResponse.Code = 0
	//	body, _ := json.Marshal(transferGodUserResponse)
	//	w.WriteHeader(http.StatusForbidden)
	//	w.Write(body)
	//	return
	//}

	fmt.Println("Create USER " + transferGodUserRequest.UserName + " Success")

	grantRoleReq := nebula_metad.NewGrantRoleReq()
	grantRoleReq.RoleItem = nebula.NewRoleItem()

	grantRoleReq.RoleItem.RoleType = nebula.RoleType_GOD
	grantRoleReq.RoleItem.User = transferGodUserRequest.UserName
	grantRoleReq.RoleItem.SpaceID = 0

	fmt.Println("Begin Grant " + transferGodUserRequest.UserName + " to GOD")
	grantRoleResp, err := metadClient.GrantRole(grantRoleReq)
	if err != nil {
		fmt.Println("Grant Roles Failed: " + err.Error())
		transferGodUserResponse.Code = ErrInitialUserFailed
		body, _ := json.Marshal(transferGodUserResponse)
		w.WriteHeader(http.StatusForbidden)
		w.Write(body)
		return
	}

	if grantRoleResp.Code != nebula_metad.ErrorCode_SUCCEEDED {
		fmt.Println("Grant Roles ErrorCode is : ", grantRoleResp.Code)
		transferGodUserResponse.Code = ErrInitialUserFailed
		body, _ := json.Marshal(transferGodUserResponse)
		w.WriteHeader(http.StatusForbidden)
		w.Write(body)
		return
	}


	dropUserReq := nebula_metad.NewDropUserReq()
	dropUserReq.Account = transferGodUserRequest.OldName
	_, err = metadClient.DropUser(dropUserReq)
	if err != nil {
		fmt.Println("MetadClient Create User Failed !", err.Error())
		transferGodUserResponse.Code = ErrInternalError
		body, _ := json.Marshal(transferGodUserResponse)
		w.WriteHeader(http.StatusForbidden)
		w.Write(body)
		return
	}


	transferGodUserResponse.Code = 0
	body, _ := json.Marshal(transferGodUserResponse)
	w.WriteHeader(http.StatusOK)
	w.Write(body)
	fmt.Println("Create GOD User " + transferGodUserRequest.UserName + " Success!")
	return
}

func InitializeHandler(w http.ResponseWriter, r *http.Request) {
	createUserRequest := CreateUserRequest{}
	createUserResponse := CreateUserResponse{}
	bodyData, err := ioutil.ReadAll(r.Body)

	if err != nil {
		createUserResponse.Code = ErrInternalError
		body, _ := json.Marshal(createUserResponse)
		w.WriteHeader(http.StatusForbidden)
		w.Write(body)
		return
	}

	json.Unmarshal(bodyData, &createUserRequest)

	metadClient, err := makeMetadClient(createUserRequest.InstanceID)
	if err != nil {
		fmt.Println("Create MetadClient Error!")
		createUserResponse.Code = ErrInternalError
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

	createUserReq.Account = createUserRequest.UserName

	createUserResp, err := metadClient.CreateUser(createUserReq)

	if err != nil {

		fmt.Println("MetadClient Create User Failed !", err.Error())
		createUserResponse.Code = ErrInternalError
		body, _ := json.Marshal(createUserResponse)
		w.WriteHeader(http.StatusForbidden)
		w.Write(body)
		return
	}

	if createUserResp.Code != nebula_metad.ErrorCode_SUCCEEDED &&
		createUserResp.Code != nebula_metad.ErrorCode_E_EXISTED {

		fmt.Println("Create User Failed")
		createUserResponse.Code = ErrUserExisted
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
		createUserResponse.Code = ErrInitialUserFailed
		body, _ := json.Marshal(createUserResponse)
		w.WriteHeader(http.StatusForbidden)
		w.Write(body)
		return
	}

	if grantRoleResp.Code != nebula_metad.ErrorCode_SUCCEEDED {
		fmt.Println("Grant Roles ErrorCode is : ", grantRoleResp.Code)
		createUserResponse.Code = ErrInitialUserFailed
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

func CreateUserHandler(w http.ResponseWriter, r *http.Request) {
	createUserRequest := CreateUserRequest{}
	createUserResponse := CreateUserResponse{}
	bodyData, err := ioutil.ReadAll(r.Body)

	if err != nil {
		fmt.Println("Invalid Request Body")
		createUserResponse.Code = ErrInvalidRequestBody
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
		createUserResponse.Code = ErrInternalError
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

	operatorRole, err := GetUserRoles(createUserRequest.Account, createUserRequest.SpaceName, createUserRequest.InstanceID)
	if err != nil {
		fmt.Println("Create User Failed ", err.Error())
		createUserResponse.Code = ErrGrantRoleFailed
		body, _ := json.Marshal(createUserResponse)
		w.Write(body)

		w.WriteHeader(http.StatusForbidden)
		return
	}
	fmt.Printf("User %s Role is %d\n", createUserRequest.Account, operatorRole)

	//userRole, err := GetUserRoles(createUserRequest.UserName, createUserRequest.SpaceName, metadClient)
	//if err == nil {
	//	roleType = userRole
	//}
	fmt.Printf("User %s Role is %d\n", createUserRequest.UserName, operatorRole)

	if operatorRole > roleType {
		fmt.Println("Create User Failed: FatherAccount Role larger then Role")
		createUserResponse.Code = ErrGrantRoleFailed
		body, _ := json.Marshal(createUserResponse)
		w.Write(body)

		w.WriteHeader(http.StatusForbidden)
		return
	}

	createUserReq := nebula_metad.NewCreateUserReq()

	createUserReq.IfNotExists = true
	createUserReq.Account = createUserRequest.UserName

	createUserResp, err := metadClient.CreateUser(createUserReq)

	if err != nil && createUserResp.Code != nebula_metad.ErrorCode_E_EXISTED {
		fmt.Println("Create User Failed ", err.Error())
		createUserResponse.Code = ErrInternalError
		body, _ := json.Marshal(createUserResponse)
		w.Write(body)

		w.WriteHeader(http.StatusForbidden)
		return
	}

	if createUserResp.Code != nebula_metad.ErrorCode_SUCCEEDED {
		if createUserResp.Code == nebula_metad.ErrorCode_E_EXISTED {

		} else {
			createUserResponse.Code = ErrInternalError
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

		createUserResponse.Code = ErrInternalError
		body, _ := json.Marshal(createUserResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	if getSpaceResp.Code != nebula_metad.ErrorCode_SUCCEEDED {
		if getSpaceResp.Code == nebula_metad.ErrorCode_E_NOT_FOUND {
			createUserResponse.Code = ErrSpaceNotFound
			body, _ := json.Marshal(createUserResponse)
			w.Write(body)
			fmt.Println("Get Space Failed ", getSpaceResp.Code)
			w.WriteHeader(http.StatusForbidden)
			return
		} else {
			createUserResponse.Code = ErrInternalError
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
		createUserResponse.Code = ErrInternalError
		body, _ := json.Marshal(createUserResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	if grantRoleResp.Code != nebula_metad.ErrorCode_SUCCEEDED {
		createUserResponse.Code = ErrInternalError
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
		deleteUserResponse.Code = ErrInvalidRequestBody
		body, _ := json.Marshal(deleteUserResponse)
		w.Write(body)

		w.WriteHeader(http.StatusForbidden)
		return
	}

	json.Unmarshal(bodyData, &deleteUserRequest)

	metadClient, err := makeMetadClient(deleteUserRequest.InstanceID)
	if err != nil {

		fmt.Println("Create Metad Client Error ", err.Error())
		deleteUserResponse.Code = ErrInternalError
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

	revokerRole, err := GetUserRoles(deleteUserRequest.Account, deleteUserRequest.Space, deleteUserRequest.InstanceID)
	if err != nil {
		fmt.Println("Delete User Failed ", err.Error())
		deleteUserResponse.Code = ErrGrantRoleFailed
		body, _ := json.Marshal(deleteUserResponse)
		w.Write(body)

		w.WriteHeader(http.StatusForbidden)
		return
	}

	if revokerRole > roleType {
		fmt.Println("Delete User Failed")
		deleteUserResponse.Code = ErrGrantRoleFailed
		body, _ := json.Marshal(deleteUserResponse)
		w.Write(body)

		w.WriteHeader(http.StatusForbidden)
		return
	}


	spaceID, err := getSpaceID(deleteUserRequest.Space, metadClient)

	if err != nil {
		fmt.Println("Get SpaceID Failed ", err.Error())
		deleteUserResponse.Code = ErrInternalError
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
		deleteUserResponse.Code = ErrInternalError
		body, _ := json.Marshal(deleteUserResponse)
		w.Write(body)

		w.WriteHeader(http.StatusForbidden)
		return
	}

	if revokeRoleResp.Code != nebula_metad.ErrorCode_SUCCEEDED {
		fmt.Println("Revoke User Failed, Responce Code: ", revokeRoleResp.Code)
		deleteUserResponse.Code = ErrInternalError
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

func rolesToString(role nebula.RoleType) string {
	if role == nebula.RoleType_GOD {
		return "GOD"
	} else if role == nebula.RoleType_ADMIN {
		return "ADMIN"
	} else if role == nebula.RoleType_DBA {
		return "DBA"
	} else if role == nebula.RoleType_USER {
		return "USER"
	} else {
		return "GUEST"
	}
}

func ListSpaceUsersHandler(w http.ResponseWriter, r *http.Request) {
	listUserRequest := ListUserRequest{}
	listUserResponse := ListUserResponse{}

	bodyData, err := ioutil.ReadAll(r.Body)

	if err != nil {
		fmt.Println("Invalid Request Body")
		listUserResponse.Code = ErrInvalidRequestBody
		body, _ := json.Marshal(listUserResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	json.Unmarshal(bodyData, &listUserRequest)

	metadClient, err := makeMetadClient(listUserRequest.InstanceID)
	if err != nil {
		fmt.Println("Create MetadClient Error ", err.Error())
		listUserResponse.Code = ErrInternalError
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

	operatorRole, err := GetUserRoles(listUserRequest.Operator, listUserRequest.SpaceName, metadClient)
	if err != nil {
		fmt.Println("Invalid Request Body")
		listUserResponse.Code = ErrNotFound
		body, _ := json.Marshal(listUserResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	if operatorRole >  nebula.RoleType_ADMIN {
		listUserResponse.Code = 0
		listUserResponse.UserRoles = make(map[string]string)
		listUserResponse.UserRoles[listUserRequest.Operator] = rolesToString(operatorRole)
		respBody, _ := json.Marshal(listUserResponse)
		fmt.Fprintf(w, string(respBody))
		return
	}

	listUserReq := nebula_metad.NewListUsersReq()

	listUserResp, err := metadClient.ListUsers(listUserReq)

	if listUserResp.Code != nebula_metad.ErrorCode_SUCCEEDED {
		fmt.Println("List User Failed")
		listUserResponse.Code = ErrInternalError
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
		listUserResponse.Code = ErrInternalError
		body, _ := json.Marshal(listUserResponse)
		w.Write(body)

		w.WriteHeader(http.StatusForbidden)
		return
	}

	if getSpaceResp.Code != nebula_metad.ErrorCode_SUCCEEDED {
		fmt.Println("Get Space Failed, ErrorCode is ", getSpaceResp.Code)
		listUserResponse.Code = ErrNotFound
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
				if nebula.RoleType_GOD == operatorRole {
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
				if nebula.RoleType_ADMIN == operatorRole {
					switch role.RoleType {
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
		listUserResponse.Code = ErrInvalidRequestBody
		body, _ := json.Marshal(listUserResponse)
		w.Write(body)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	json.Unmarshal(bodyData, &listUserRequest)

	metadClient, err := makeMetadClient(listUserRequest.InstanceID)
	if err != nil {
		fmt.Println("Create MetadClient Error ", err.Error())
		listUserResponse.Code = ErrInternalError
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
		listUserResponse.Code = ErrInternalError
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

