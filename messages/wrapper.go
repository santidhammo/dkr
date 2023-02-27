package messages

import (
	pb "github.com/santidhammo/dkr/remotesapi"

	"google.golang.org/protobuf/proto"
)

func WrapGetRepoMetadataRequest(in *pb.GetRepoMetadataRequest) (*Message, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewTrackedMessage(GetRepoMetadata, payload), nil
		}
	}
	return nil, nil
}

func WrapGetRepoMetadataResponse(in *pb.GetRepoMetadataResponse, id MessageID) (*Message, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewResponseMessage(GetRepoMetadata, payload, id), nil
		}
	}
	return nil, nil
}

func WrapRootRequest(in *pb.RootRequest) (*Message, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewTrackedMessage(Root, payload), nil
		}
	}
	return nil, nil
}

func WrapRootResponse(in *pb.RootResponse, id MessageID) (*Message, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewResponseMessage(Root, payload, id), nil
		}
	}
	return nil, nil
}

func WrapHasChunksRequest(in *pb.HasChunksRequest) (*Message, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewTrackedMessage(HasChunks, payload), nil
		}
	}
	return nil, nil
}

func WrapGetDownloadLocsRequest(in *pb.GetDownloadLocsRequest) (*Message, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewTrackedMessage(GetDownloadLocations, payload), nil
		}
	}
	return nil, nil
}

func WrapGetUploadLocsRequest(in *pb.GetUploadLocsRequest) (*Message, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewTrackedMessage(GetUploadLocations, payload), nil
		}
	}
	return nil, nil
}

func WrapRebaseRequest(in *pb.RebaseRequest) (*Message, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewTrackedMessage(Rebase, payload), nil
		}
	}
	return nil, nil
}

func WrapCommitRequest(in *pb.CommitRequest) (*Message, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewTrackedMessage(Commit, payload), nil
		}
	}
	return nil, nil
}

func WrapListTableFilesRequest(in *pb.ListTableFilesRequest) (*Message, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewTrackedMessage(ListTableFiles, payload), nil
		}
	}
	return nil, nil
}

func WrapRefreshTableFileUrlRequest(in *pb.RefreshTableFileUrlRequest) (*Message, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewTrackedMessage(RefreshTableFileUrl, payload), nil
		}
	}
	return nil, nil
}

func WrapAddTableFilesRequest(in *pb.AddTableFilesRequest) (*Message, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewTrackedMessage(AddTableFiles, payload), nil
		}
	}
	return nil, nil
}

func WrapStreamingDownloadLocsRequest(in *pb.GetDownloadLocsRequest, id MessageID) (*Message, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewStreamingMessage(StreamDownloadLocations, payload, id), nil
		}
	}
	return nil, nil
}

func WrapStreamingDownloadLocsResponse(in *pb.GetDownloadLocsResponse, id MessageID) (*Message, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewStreamingMessage(StreamDownloadLocations, payload, id), nil
		}
	}
	return nil, nil
}
