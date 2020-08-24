package utils

import (
	"fmt"
	"context"
	"log"
	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"github.com/vesoft-inc/nebula-go/nebula"
	nebula_metad "github.com/vesoft-inc/nebula-go/nebula/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"time"
)

var client *kubernetes.Clientset

func SetK8sClient(cli *kubernetes.Clientset) {
	client = cli
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

func DropUser(ns, user string) error {
	metadClient, err := makeMetadClient(ns)
	if err != nil {
		return err
	}

	defer func() {
		if metadClient != nil {
			metadClient.Transport.Close()
		}
	}()

	dropUserReq := nebula_metad.NewDropUserReq()
	dropUserReq.Account = user
	_, err = metadClient.DropUser(dropUserReq)
	if err != nil {
		fmt.Println("MetadClient Drop User Failed !", err.Error())
		return err
	}

	return nil
}

func CreateUser(ns, user string) error {
	metadClient, err := makeMetadClient(ns)
	if err != nil {
		return err
	}

	defer func() {
		if metadClient != nil {
			metadClient.Transport.Close()
		}
	}()

	createUserReq := nebula_metad.NewCreateUserReq()
	createUserReq.Account = user
	_, err = metadClient.CreateUser(createUserReq)
	if err != nil {
		fmt.Println("MetadClient Create User Failed !", err.Error())
		return err
	}

	return nil
}

func ListUsers(ns string) ([]string, error ) {
	metadClient, err := makeMetadClient(ns)
	if err != nil {
		return []string{}, err
	}

	defer func() {
		if metadClient != nil {
			metadClient.Transport.Close()
		}
	}()

	listUsersResp, err := metadClient.ListUsers(&nebula_metad.ListUsersReq{})

	if err != nil {
		return []string{}, err
	}

	users := listUsersResp.Users

	res := []string{}

	for user, _ := range users {
		res = append(res, user)
	}

	return res, nil
}

func IsUserInSpace(spaceName, userName, ns string) bool {
	metadClient, err := makeMetadClient(ns)
	if err != nil {
		log.Println("Create Metad Client Error: %v", err)
		return false
	}

	defer func() {
		if metadClient != nil {
			metadClient.Transport.Close()
		}
	}()

	_, err = GetUserRoles(userName, spaceName, ns)
	if err != nil {

		return false
	}
	return true
}

func GetUserRoles(user, spaceName, ns string) (nebula.RoleType, error) {
	metadClient, err := makeMetadClient(ns)
	if err != nil {
		log.Println("Create Metad Client Error: %v", err)
		return nebula.RoleType_GUEST, fmt.Errorf("Internal Error")
	}

	defer func() {
		if metadClient != nil {
			metadClient.Transport.Close()
		}
	}()

	//是否是god用户
	spaceID := nebula.GraphSpaceID(0)
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

	spaceID, err = GetSpaceID(ns, spaceName)
	if err != nil {
		fmt.Println("List User Failed")
		return -1, fmt.Errorf("Inner Error")
	}

	getUserRolesReq = nebula_metad.NewGetUserRolesReq()
	getUserRolesReq.Account = user

	roleResp, err = metadClient.GetUserRoles(getUserRolesReq)
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

	return nebula.RoleType_GUEST, fmt.Errorf("Not Found")
}

func GetSpaceID(ns, spaceName string) (nebula.GraphSpaceID, error) {
	getSpaceReq := nebula_metad.NewGetSpaceReq()
	fmt.Println("SpaceName is ", spaceName)
	getSpaceReq.SpaceName = spaceName
	metadClient, err := makeMetadClient(ns)
	if err != nil {
		log.Println("Create Metad Client Error: %v", err)
		return 0, fmt.Errorf("Internal Error")
	}

	defer func() {
		if metadClient != nil {
			metadClient.Transport.Close()
		}
	}()


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

func ListSpaces(ns string) ([]string, error ) {
	metadClient, err := makeMetadClient(ns)
	if err != nil {
		return []string{}, err
	}

	defer func() {
		if metadClient != nil {
			metadClient.Transport.Close()
		}
	}()

	listSpacesResp, err := metadClient.ListSpaces(&nebula_metad.ListSpacesReq{})

	if err != nil {
		return []string{}, err
	}

	ids := listSpacesResp.GetSpaces()

	res := []string{}

	for _, id := range ids {
		res = append(res, id.Name)
	}

	return res, nil
}