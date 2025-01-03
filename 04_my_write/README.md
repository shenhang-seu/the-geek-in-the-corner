这是一个server往client写数据的demo：

[root@node151 04_my_write]# ./rdma-server write
listening on port 41391.
received connection request.
server first received client's MR and send back.
server send MR back to client successfully.
server writing message to client's memory...
server send rdma operation to client successfully.
server inform client rdma operation finish(server send MSG_SERVER_DONE to client).
server send MSG_SERVER_DONE to client successfully.
receive client's MSG_DISCONNECT successfully
server send MSG_DISCONNECT to client and wait disconnect...
server send MSG_DISCONNECT to client successfully.
peer disconnected.

[root@node151 04_my_write]# ./rdma-client write 172.100.157.151 41391
address resolved.
route resolved.
client send MR to server successfully.
client received server's MR.
client inform server to operate client's memory(client send MSG_CLIENT_READY to server).
client send MSG_CLIENT_READY to server successfully.
client receive server's MSG_SERVER_DONE successfully.
client's remote buffer: message from passive/server side with pid 4092336
client send MSG_DISCONNECT to server.
client send MSG_DISCONNECT to server successfully.
client receive server's MSG_DISCONNECT successfully.
disconnected.
[root@node151 04_my_write]#

