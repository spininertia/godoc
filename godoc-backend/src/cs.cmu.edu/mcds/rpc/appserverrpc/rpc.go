package appserverrpc

type RemoteAppServer interface {
	// Pause current AppServer, queue all requests from clients
	Pause(args *PauseArgs, reply *PauseReply) error

	// Notify the newly added server to update state
	UpdateState(args *UpdateStateArgs, reply *UpdateStateReply) error

	// The newly added Server call this rpc to get the up-to-date server state
	CopyState(args *CopyStateArgs, reply *CopyStateReply) error

	// Notifies AppServer to replace old server addr with the new one
	Replace(args *ReplaceArgs, reply *ReplaceArgs) error

	// Notifies AppServer to stop pausing, start dealing with client request
	Restart(args *RestartArgs, reply *RestartReply) error
}

type AppServer struct {
	// embed all methods into the struct
	RemoteAppServer
}

// Wrap wraps s in a type-safe wrapper struct to ensure that only
// AppServer methods are exported to receive RPCS
func Wrap(s RemoteAppServer) RemoteAppServer {
	return &AppServer{s}
}
