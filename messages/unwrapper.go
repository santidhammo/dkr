package messages

import (
	pb "github.com/santidhammo/dkr/remotesapi"

	"google.golang.org/protobuf/proto"
)

func UnWrapGetRepoMetadataRequest(message *RPCMessage) (*pb.GetRepoMetadataRequest, error) {
	if message != nil {
		var request pb.GetRepoMetadataRequest
		err := proto.Unmarshal(message.Payload, &request)
		if err != nil {
			return nil, err
		} else {
			return &request, nil
		}
	}
	return nil, nil
}

func UnWrapGetRepoMetadataResponse(message *RPCMessage) (*pb.GetRepoMetadataResponse, error) {
	if message != nil {
		var response pb.GetRepoMetadataResponse
		err := proto.Unmarshal(message.Payload, &response)
		if err != nil {
			return nil, err
		} else {
			return &response, nil
		}
	}
	return nil, nil
}

func UnWrapRootRequest(message *RPCMessage) (*pb.RootRequest, error) {
	if message != nil {
		var request pb.RootRequest
		err := proto.Unmarshal(message.Payload, &request)
		if err != nil {
			return nil, err
		} else {
			return &request, nil
		}
	}
	return nil, nil
}

func UnWrapRootResponse(message *RPCMessage) (*pb.RootResponse, error) {
	if message != nil {
		var response pb.RootResponse
		err := proto.Unmarshal(message.Payload, &response)
		if err != nil {
			return nil, err
		} else {
			return &response, nil
		}
	}
	return nil, nil
}

func UnWrapStreamingDownloadLocsRequest(message *RPCMessage) (*pb.GetDownloadLocsRequest, error) {
	if message != nil {
		var request pb.GetDownloadLocsRequest
		err := proto.Unmarshal(message.Payload, &request)
		if err != nil {
			return nil, err
		} else {
			return &request, nil
		}
	}
	return nil, nil
}

func UnWrapStreamingDownloadLocsResponse(message *RPCMessage) (*pb.GetDownloadLocsResponse, error) {
	if message != nil {
		var response pb.GetDownloadLocsResponse
		err := proto.Unmarshal(message.Payload, &response)
		if err != nil {
			return nil, err
		} else {
			return &response, nil
		}
	}
	return nil, nil
}

func UnWrapHasChunksResponse(message *RPCMessage) (*pb.HasChunksResponse, error) {
	if message != nil {
		var response pb.HasChunksResponse
		err := proto.Unmarshal(message.Payload, &response)
		if err != nil {
			return nil, err
		} else {
			return &response, nil
		}
	}
	return nil, nil
}

func UnWrapHasChunksRequest(message *RPCMessage) (*pb.HasChunksRequest, error) {
	if message != nil {
		var request pb.HasChunksRequest
		err := proto.Unmarshal(message.Payload, &request)
		if err != nil {
			return nil, err
		} else {
			return &request, nil
		}
	}
	return nil, nil
}

func UnWrapListTableFilesResponse(message *RPCMessage) (*pb.ListTableFilesResponse, error) {
	if message != nil {
		var response pb.ListTableFilesResponse
		err := proto.Unmarshal(message.Payload, &response)
		if err != nil {
			return nil, err
		} else {
			return &response, nil
		}
	}
	return nil, nil
}

func UnWrapListTableFilesRequest(message *RPCMessage) (*pb.ListTableFilesRequest, error) {
	if message != nil {
		var request pb.ListTableFilesRequest
		err := proto.Unmarshal(message.Payload, &request)
		if err != nil {
			return nil, err
		} else {
			return &request, nil
		}
	}
	return nil, nil
}

func UnWrapGetUploadLocsResponse(message *RPCMessage) (*pb.GetUploadLocsResponse, error) {
	if message != nil {
		var response pb.GetUploadLocsResponse
		err := proto.Unmarshal(message.Payload, &response)
		if err != nil {
			return nil, err
		} else {
			return &response, nil
		}
	}
	return nil, nil
}

func UnWrapGetUploadLocsRequest(message *RPCMessage) (*pb.GetUploadLocsRequest, error) {
	if message != nil {
		var request pb.GetUploadLocsRequest
		err := proto.Unmarshal(message.Payload, &request)
		if err != nil {
			return nil, err
		} else {
			return &request, nil
		}
	}
	return nil, nil
}

func UnWrapAddTableFilesResponse(message *RPCMessage) (*pb.AddTableFilesResponse, error) {
	if message != nil {
		var response pb.AddTableFilesResponse
		err := proto.Unmarshal(message.Payload, &response)
		if err != nil {
			return nil, err
		} else {
			return &response, nil
		}
	}
	return nil, nil
}

func UnWrapAddTableFilesRequest(message *RPCMessage) (*pb.AddTableFilesRequest, error) {
	if message != nil {
		var request pb.AddTableFilesRequest
		err := proto.Unmarshal(message.Payload, &request)
		if err != nil {
			return nil, err
		} else {
			return &request, nil
		}
	}
	return nil, nil
}

func UnWrapCommitRequest(message *RPCMessage) (*pb.CommitRequest, error) {
	if message != nil {
		var request pb.CommitRequest
		err := proto.Unmarshal(message.Payload, &request)
		if err != nil {
			return nil, err
		} else {
			return &request, nil
		}
	}
	return nil, nil
}

func UnWrapCommitResponse(message *RPCMessage) (*pb.CommitResponse, error) {
	if message != nil {
		var response pb.CommitResponse
		err := proto.Unmarshal(message.Payload, &response)
		if err != nil {
			return nil, err
		} else {
			return &response, nil
		}
	}
	return nil, nil
}

func UnWrapRebaseRequest(message *RPCMessage) (*pb.RebaseRequest, error) {
	if message != nil {
		var request pb.RebaseRequest
		err := proto.Unmarshal(message.Payload, &request)
		if err != nil {
			return nil, err
		} else {
			return &request, nil
		}
	}
	return nil, nil
}

func UnWrapRebaseResponse(message *RPCMessage) (*pb.RebaseResponse, error) {
	if message != nil {
		var response pb.RebaseResponse
		err := proto.Unmarshal(message.Payload, &response)
		if err != nil {
			return nil, err
		} else {
			return &response, nil
		}
	}
	return nil, nil
}

func UnWrapGetDownloadLocsRequest(message *RPCMessage) (*pb.GetDownloadLocsRequest, error) {
	if message != nil {
		var request pb.GetDownloadLocsRequest
		err := proto.Unmarshal(message.Payload, &request)
		if err != nil {
			return nil, err
		} else {
			return &request, nil
		}
	}
	return nil, nil
}

func UnWrapGetDownloadLocsResponse(message *RPCMessage) (*pb.GetDownloadLocsResponse, error) {
	if message != nil {
		var response pb.GetDownloadLocsResponse
		err := proto.Unmarshal(message.Payload, &response)
		if err != nil {
			return nil, err
		} else {
			return &response, nil
		}
	}
	return nil, nil
}

func UnWrapRefreshTableFileUrlRequest(message *RPCMessage) (*pb.RefreshTableFileUrlRequest, error) {
	if message != nil {
		var request pb.RefreshTableFileUrlRequest
		err := proto.Unmarshal(message.Payload, &request)
		if err != nil {
			return nil, err
		} else {
			return &request, nil
		}
	}
	return nil, nil
}

func UnWrapRefreshTableFileUrlResponse(message *RPCMessage) (*pb.RefreshTableFileUrlResponse, error) {
	if message != nil {
		var response pb.RefreshTableFileUrlResponse
		err := proto.Unmarshal(message.Payload, &response)
		if err != nil {
			return nil, err
		} else {
			return &response, nil
		}
	}
	return nil, nil
}
