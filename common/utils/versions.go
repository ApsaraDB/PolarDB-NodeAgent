/*
 * Copyright (c) 2018. Alibaba Cloud, All right reserved.
 * This software is the confidential and proprietary information of Alibaba Cloud ("Confidential Information").
 * You shall not disclose such Confidential Information and shall use it only in accordance with the terms of
 * the license agreement you entered into with Alibaba Cloud.
 */

package utils

import "fmt"

var (
	RpmName     string
	RpmRelease  string
	RpmVersion  string
	GitBranch   string
	GitCommitID string
	Buildtime   string
)

func ShowVersion() string {
	versionmap := map[string]string{
		"RpmName":     RpmName,
		"RpmRelease":  RpmRelease,
		"RpmVersion":  RpmVersion,
		"GitBranch":   GitBranch,
		"GitCommitID": GitCommitID,
		"Buildtime":   Buildtime,
	}

	return fmt.Sprintf("%+v", versionmap)
}
