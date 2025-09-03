// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package tcp

import (
	binaryserialization "github.com/apache/iggy/foreign/go/binary_serialization"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

func (tms *IggyTcpClient) LoginUser(username string, password string) (*iggcon.IdentityInfo, error) {
	serializedRequest := binaryserialization.TcpLogInRequest{
		Username: username,
		Password: password,
	}
	buffer, err := tms.sendAndFetchResponse(serializedRequest.Serialize(), iggcon.LoginUserCode)
	if err != nil {
		return nil, err
	}

	return binaryserialization.DeserializeLogInResponse(buffer), nil
}

func (tms *IggyTcpClient) LoginWithPersonalAccessToken(token string) (*iggcon.IdentityInfo, error) {
	message := binaryserialization.SerializeLoginWithPersonalAccessToken(iggcon.LoginWithPersonalAccessTokenRequest{
		Token: token,
	})
	buffer, err := tms.sendAndFetchResponse(message, iggcon.LoginWithAccessTokenCode)
	if err != nil {
		return nil, err
	}

	return binaryserialization.DeserializeLogInResponse(buffer), nil
}

func (tms *IggyTcpClient) LogoutUser() error {
	_, err := tms.sendAndFetchResponse([]byte{}, iggcon.LogoutUserCode)
	return err
}
