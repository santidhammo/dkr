package messages

import (
	pb "github.com/santidhammo/dkr/remotesapi"

	"google.golang.org/protobuf/proto"
)

func UnWrapGetRepoMetadataRequest(message *Message) (*pb.GetRepoMetadataRequest, error) {
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

func UnWrapGetRepoMetadataResponse(message *Message) (*pb.GetRepoMetadataResponse, error) {
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

func UnWrapRootRequest(message *Message) (*pb.RootRequest, error) {
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

func UnWrapRootResponse(message *Message) (*pb.RootResponse, error) {
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

func UnWrapStreamingDownloadLocsRequest(message *Message) (*pb.GetDownloadLocsRequest, error) {
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

func UnWrapStreamingDownloadLocsResponse(message *Message) (*pb.GetDownloadLocsResponse, error) {
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

func UnWrapHasChunksResponse(message *Message) (*pb.HasChunksResponse, error) {
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

func UnWrapGetDownloadLocsResponse(message *Message) (*pb.GetDownloadLocsResponse, error) {
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

func UnWrapGetUploadLocsResponse(message *Message) (*pb.GetUploadLocsResponse, error) {
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

func UnWrapRebaseResponse(message *Message) (*pb.RebaseResponse, error) {
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

func UnWrapCommitResponse(message *Message) (*pb.CommitResponse, error) {
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

func UnWrapListTableFilesResponse(message *Message) (*pb.ListTableFilesResponse, error) {
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

func UnWrapRefreshTableFileUrlResponse(message *Message) (*pb.RefreshTableFileUrlResponse, error) {
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

func UnWrapAddTableFilesResponse(message *Message) (*pb.AddTableFilesResponse, error) {
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
