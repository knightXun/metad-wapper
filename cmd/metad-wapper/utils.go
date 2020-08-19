package main

import (
	"fmt"
	"github.com/astaxie/beego/logs"
	"github.com/vesoft-inc/nebula-go/nebula"
	"log"
	nebula_metad "github.com/vesoft-inc/nebula-go/nebula/meta"
)

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

func isUserInSpace(spaceName, userName, ns string) bool {
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

	spaceID, err = getSpaceID(spaceName, metadClient)
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