package messages

import (
	pb "github.com/santidhammo/dkr/remotesapi"

	"google.golang.org/protobuf/proto"
)

func WrapGetRepoMetadataRequest(in *pb.GetRepoMetadataRequest) (*RPCMessage, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewTrackedRPCMessage(GetRepoMetadata, payload), nil
		}
	}
	return nil, nil
}

func WrapGetRepoMetadataResponse(in *pb.GetRepoMetadataResponse, id MessageID) (*RPCMessage, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewResponseRPCMessage(GetRepoMetadata, payload, id), nil
		}
	}
	return nil, nil
}

func WrapRootRequest(in *pb.RootRequest) (*RPCMessage, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewTrackedRPCMessage(Root, payload), nil
		}
	}
	return nil, nil
}

func WrapRootResponse(in *pb.RootResponse, id MessageID) (*RPCMessage, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewResponseRPCMessage(Root, payload, id), nil
		}
	}
	return nil, nil
}

func WrapHasChunksRequest(in *pb.HasChunksRequest) (*RPCMessage, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewTrackedRPCMessage(HasChunks, payload), nil
		}
	}
	return nil, nil
}

func WrapHasChunksResponse(in *pb.HasChunksResponse, id MessageID) (*RPCMessage, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewResponseRPCMessage(HasChunks, payload, id), nil
		}
	}
	return nil, nil
}

func WrapListTableFilesRequest(in *pb.ListTableFilesRequest) (*RPCMessage, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewTrackedRPCMessage(ListTableFiles, payload), nil
		}
	}
	return nil, nil
}

func WrapListTableFilesResponse(in *pb.ListTableFilesResponse, id MessageID) (*RPCMessage, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewResponseRPCMessage(ListTableFiles, payload, id), nil
		}
	}
	return nil, nil
}

func WrapGetUploadLocsRequest(in *pb.GetUploadLocsRequest) (*RPCMessage, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewTrackedRPCMessage(GetUploadLocations, payload), nil
		}
	}
	return nil, nil
}

func WrapGetUploadLocsResponse(in *pb.GetUploadLocsResponse, id MessageID) (*RPCMessage, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewResponseRPCMessage(GetUploadLocations, payload, id), nil
		}
	}
	return nil, nil
}

func WrapAddTableFilesRequest(in *pb.AddTableFilesRequest) (*RPCMessage, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewTrackedRPCMessage(AddTableFiles, payload), nil
		}
	}
	return nil, nil
}

func WrapAddTableFilesResponse(in *pb.AddTableFilesResponse, id MessageID) (*RPCMessage, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewResponseRPCMessage(AddTableFiles, payload, id), nil
		}
	}
	return nil, nil
}

func WrapCommitRequest(in *pb.CommitRequest) (*RPCMessage, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewTrackedRPCMessage(Commit, payload), nil
		}
	}
	return nil, nil
}

func WrapCommitResponse(in *pb.CommitResponse, id MessageID) (*RPCMessage, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewResponseRPCMessage(Commit, payload, id), nil
		}
	}
	return nil, nil
}
func WrapRebaseRequest(in *pb.RebaseRequest) (*RPCMessage, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewTrackedRPCMessage(Rebase, payload), nil
		}
	}
	return nil, nil
}

func WrapRebaseResponse(in *pb.RebaseResponse, id MessageID) (*RPCMessage, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewResponseRPCMessage(Rebase, payload, id), nil
		}
	}
	return nil, nil
}

func WrapGetDownloadLocsRequest(in *pb.GetDownloadLocsRequest) (*RPCMessage, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewTrackedRPCMessage(GetDownloadLocations, payload), nil
		}
	}
	return nil, nil
}

func WrapGetDownloadLocsResponse(in *pb.GetDownloadLocsResponse, id MessageID) (*RPCMessage, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewResponseRPCMessage(GetDownloadLocations, payload, id), nil
		}
	}
	return nil, nil
}

func WrapRefreshTableFileUrlRequest(in *pb.RefreshTableFileUrlRequest) (*RPCMessage, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewTrackedRPCMessage(RefreshTableFileUrl, payload), nil
		}
	}
	return nil, nil
}

func WrapRefreshTableFileUrlResponse(in *pb.RefreshTableFileUrlResponse, id MessageID) (*RPCMessage, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewResponseRPCMessage(RefreshTableFileUrl, payload, id), nil
		}
	}
	return nil, nil
}

func WrapStreamingDownloadLocsRequest(in *pb.GetDownloadLocsRequest, id MessageID) (*RPCMessage, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewStreamingRPCMessage(StreamDownloadLocations, payload, id), nil
		}
	}
	return nil, nil
}

func WrapStreamingDownloadLocsResponse(in *pb.GetDownloadLocsResponse, id MessageID) (*RPCMessage, error) {
	if in != nil {
		payload, err := proto.Marshal(in)
		if err != nil {
			return nil, err
		} else if payload != nil {
			return NewStreamingRPCMessage(StreamDownloadLocations, payload, id), nil
		}
	}
	return nil, nil
}
