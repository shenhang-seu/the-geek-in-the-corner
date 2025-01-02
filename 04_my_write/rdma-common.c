#include "rdma-common.h"
#include <stdbool.h>

static const int RDMA_BUFFER_SIZE = 1024;

struct message {
	enum {
		MSG_MR,
		MSG_DONE,
		MSG_DISCONNECT,
		MSG_RDMA_OPERATION
	} type;

	union {
		struct ibv_mr mr;
	} data;
};

struct context {
	struct ibv_context* ctx;
	struct ibv_pd* pd;
	struct ibv_cq* cq;
	struct ibv_comp_channel* comp_channel;

	pthread_t cq_poller_thread;
};

struct connection {
	struct rdma_cm_id* id;
	struct ibv_qp* qp;

	int connected;

	struct ibv_mr* recv_mr;
	struct ibv_mr* send_mr;
	struct ibv_mr* rdma_local_mr;
	struct ibv_mr* rdma_remote_mr;

	struct ibv_mr peer_mr;

	struct message* recv_msg;
	struct message* send_msg;

	char* rdma_local_region;
	char* rdma_remote_region;

	enum {
		SS_INIT,
		SS_MR_SENT,
		SS_RDMA_SENT,
		SS_DONE_SENT
	} send_state;

	enum {
		RS_INIT,
		RS_MR_RECV,
		RS_DONE_RECV
	} recv_state;
};

static void server_build_context(struct ibv_context* verbs);
static void client_build_context(struct ibv_context* verbs);
static void build_qp_attr(struct ibv_qp_init_attr* qp_attr);
//static char * get_peer_message_region(struct connection *conn);
static void server_on_completion(struct ibv_wc*);
static void client_on_completion(struct ibv_wc*);
static void* server_poll_cq(void*);
static void* client_poll_cq(void*);
static void post_receives(struct connection* conn);
static void register_memory(struct connection* conn);
static void send_message(struct connection* conn);

static struct context* s_ctx = NULL;
static enum mode s_mode = M_WRITE;

void die(const char* reason)
{
	fprintf(stderr, "%s\n", reason);
	exit(EXIT_FAILURE);
}

void server_build_connection(struct rdma_cm_id* id)
{
	struct connection* conn;
	struct ibv_qp_init_attr qp_attr;

	server_build_context(id->verbs);
	build_qp_attr(&qp_attr);

	TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));

	id->context = conn = (struct connection*)malloc(sizeof(struct connection));

	conn->id = id;
	conn->qp = id->qp;

	conn->send_state = SS_INIT;
	conn->recv_state = RS_INIT;

	conn->connected = 0;

	register_memory(conn);
	post_receives(conn);
}

void client_build_connection(struct rdma_cm_id* id)
{
	struct connection* conn;
	struct ibv_qp_init_attr qp_attr;

	client_build_context(id->verbs);
	build_qp_attr(&qp_attr);

	TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));

	id->context = conn = (struct connection*)malloc(sizeof(struct connection));

	conn->id = id;
	conn->qp = id->qp;

	conn->send_state = SS_INIT;
	conn->recv_state = RS_INIT;

	conn->connected = 0;

	register_memory(conn);
	post_receives(conn);
}


void server_build_context(struct ibv_context* verbs)
{
	if (s_ctx) {
		if (s_ctx->ctx != verbs)
			die("cannot handle events in more than one context.");

		return;
	}

	s_ctx = (struct context*)malloc(sizeof(struct context));

	s_ctx->ctx = verbs;

	TEST_Z(s_ctx->pd = ibv_alloc_pd(s_ctx->ctx));
	TEST_Z(s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx));
	TEST_Z(s_ctx->cq = ibv_create_cq(s_ctx->ctx, 10, NULL, s_ctx->comp_channel, 0)); /* cqe=10 is arbitrary */
	TEST_NZ(ibv_req_notify_cq(s_ctx->cq, 0));

	TEST_NZ(pthread_create(&s_ctx->cq_poller_thread, NULL, server_poll_cq, NULL));
}

void client_build_context(struct ibv_context* verbs)
{
	if (s_ctx) {
		if (s_ctx->ctx != verbs)
			die("cannot handle events in more than one context.");

		return;
	}

	s_ctx = (struct context*)malloc(sizeof(struct context));

	s_ctx->ctx = verbs;

	TEST_Z(s_ctx->pd = ibv_alloc_pd(s_ctx->ctx));
	TEST_Z(s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx));
	TEST_Z(s_ctx->cq = ibv_create_cq(s_ctx->ctx, 10, NULL, s_ctx->comp_channel, 0)); /* cqe=10 is arbitrary */
	TEST_NZ(ibv_req_notify_cq(s_ctx->cq, 0));

	TEST_NZ(pthread_create(&s_ctx->cq_poller_thread, NULL, client_poll_cq, NULL));
}


void build_params(struct rdma_conn_param* params)
{
	memset(params, 0, sizeof(*params));

	params->initiator_depth = params->responder_resources = 1;
	params->rnr_retry_count = 7; /* infinite retry */
}

void build_qp_attr(struct ibv_qp_init_attr* qp_attr)
{
	memset(qp_attr, 0, sizeof(*qp_attr));

	qp_attr->send_cq = s_ctx->cq;
	qp_attr->recv_cq = s_ctx->cq;
	qp_attr->qp_type = IBV_QPT_RC;

	qp_attr->cap.max_send_wr = 10;
	qp_attr->cap.max_recv_wr = 10;
	qp_attr->cap.max_send_sge = 1;
	qp_attr->cap.max_recv_sge = 1;
}

void destroy_connection(void* context)
{
	struct connection* conn = (struct connection*)context;

	rdma_destroy_qp(conn->id);

	ibv_dereg_mr(conn->send_mr);
	ibv_dereg_mr(conn->recv_mr);
	ibv_dereg_mr(conn->rdma_local_mr);
	ibv_dereg_mr(conn->rdma_remote_mr);

	free(conn->send_msg);
	free(conn->recv_msg);
	free(conn->rdma_local_region);
	free(conn->rdma_remote_region);

	rdma_destroy_id(conn->id);

	free(conn);
}

void* get_local_message_region(void* context)
{
	if (s_mode == M_WRITE)
		return ((struct connection*)context)->rdma_local_region;
	else
		return ((struct connection*)context)->rdma_remote_region;
}
/*
char * get_peer_message_region(struct connection *conn)
{
  if (s_mode == M_WRITE)
	return conn->rdma_remote_region;
  else
	return conn->rdma_local_region;
}
*/

void server_on_completion(struct ibv_wc* wc)
{
	struct connection* conn = (struct connection*)(uintptr_t)wc->wr_id;

	if (wc->status != IBV_WC_SUCCESS)
		die("server_on_completion: status is not IBV_WC_SUCCESS.");

	if (wc->opcode & IBV_WC_RECV) {
		conn->recv_state++;

		if (conn->recv_msg->type == MSG_MR) {
			memcpy(&conn->peer_mr, &conn->recv_msg->data.mr, sizeof(conn->peer_mr));
			post_receives(conn); /* only rearm for MSG_MR */

			printf("server first received client's MR and send back.\n");
			send_mr(conn);
		}
		else if (conn->recv_msg->type == MSG_DONE) {
			post_receives(conn); /* rearm for MSG_DONE，因为后续还要接收client发来的MSG_DISCONNECT */

			struct ibv_send_wr wr, * bad_wr = NULL;
			struct ibv_sge sge;

			if (s_mode == M_WRITE)
			{
			    conn->send_msg->type = MSG_RDMA_OPERATION;
				printf("server writing message to client's memory...\n"); //server往client的rdma_remote_region写入数据
				memset(&wr, 0, sizeof(wr));

				wr.wr_id = (uintptr_t)conn;
				wr.opcode = (s_mode == M_WRITE) ? IBV_WR_RDMA_WRITE : IBV_WR_RDMA_READ;//在IBV_WR_RDMA_WRITE这种类型中，remote QP不需要调用ibv_post_recv，因为发送方知道数据写到remote node的详细位置
				wr.sg_list = &sge;
				wr.num_sge = 1;
				wr.send_flags = IBV_SEND_SIGNALED;
				wr.wr.rdma.remote_addr = (uintptr_t)conn->peer_mr.addr;
				wr.wr.rdma.rkey = conn->peer_mr.rkey;

				sge.addr = (uintptr_t)conn->rdma_local_region;
				sge.length = RDMA_BUFFER_SIZE;
				sge.lkey = conn->rdma_local_mr->lkey;

				TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
			}
			else
			{
				printf("server reading message from client's memory...\n");//本例子只实现写
			}
		}
		else if (conn->recv_msg->type == MSG_DISCONNECT)
		{
			printf("receive client's MSG_DISCONNECT successfully\n");
		}
		else
		{
			printf("receive completed successfully\n");
		}

	}
	else {
		if (SS_INIT == conn->send_state)
		{
			printf("server send MR back to client completed successfully.\n");
		}
		else if (MSG_RDMA_OPERATION == conn->send_msg->type)
		{
			printf("server send rdma operation to client completed successfully.\n");
			conn->send_msg->type = MSG_DONE;
			send_message(conn);
			printf("server inform client rdma operation finish(server send MSG_DONE to client).\n");
		}
		else if (MSG_DONE == conn->send_msg->type)
		{
			printf("server send MSG_DONE to client completed successfully.\n");
		}
		else
		{
			printf("send completed successfully.\n");
		}
		conn->send_state++;
	}
}

void client_on_completion(struct ibv_wc* wc)
{
	struct connection* conn = (struct connection*)(uintptr_t)wc->wr_id;

	if (wc->status != IBV_WC_SUCCESS)
		die("client_on_completion: status is not IBV_WC_SUCCESS.");

	if (wc->opcode & IBV_WC_RECV) {
		conn->recv_state++;

		if (conn->recv_msg->type == MSG_MR) {
			printf("client received server's MR.\n");
			memcpy(&conn->peer_mr, &conn->recv_msg->data.mr, sizeof(conn->peer_mr));
			post_receives(conn); /* only rearm for MSG_MR */
		}
		else if (conn->recv_msg->type == MSG_DONE) {
			if (s_mode == M_WRITE)
			{
				printf("client receive server's MSG_DONE completed successfully.\n");
				printf("client's remote buffer: %s\n", conn->rdma_remote_region); //client访问server写入的数据

				printf("client send MSG_DISCONNECT to server.\n");
				conn->send_msg->type = MSG_DISCONNECT;
				send_message(conn);
			}
			rdma_disconnect(conn->id);
		}
		else
		{
			printf("receive completed successfully\n");
		}

	}
	else {
		if (SS_INIT == conn->send_state)
		{
			printf("client send MR to server completed successfully.\n");
		}
		else if (conn->send_msg->type == MSG_DONE)
		{
			printf("client send MSG_DONE to server completed successfully.\n");
		}
		else if (conn->send_msg->type == MSG_DISCONNECT)
		{
			printf("client send MSG_DISCONNECT to server completed successfully.\n");
		}
		else
		{
			printf("send completed successfully.\n");
		}
		conn->send_state++;
	}

	if (conn->send_state == SS_MR_SENT && conn->recv_state == RS_MR_RECV) {
		conn->send_msg->type = MSG_DONE;
		send_message(conn);
		printf("client inform server to operate client's memory(client send MSG_DONE to server).\n");
	}
}


void on_connect(void* context)
{
	((struct connection*)context)->connected = 1;
}

void* server_poll_cq(void* ctx)
{
	struct ibv_cq* cq;
	struct ibv_wc wc;

	while (1) {
		TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
		ibv_ack_cq_events(cq, 1);
		TEST_NZ(ibv_req_notify_cq(cq, 0));

		while (ibv_poll_cq(cq, 1, &wc))
			server_on_completion(&wc);
	}

	return NULL;
}

void* client_poll_cq(void* ctx)
{
	struct ibv_cq* cq;
	struct ibv_wc wc;

	while (1) {
		TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
		ibv_ack_cq_events(cq, 1);
		TEST_NZ(ibv_req_notify_cq(cq, 0));

		while (ibv_poll_cq(cq, 1, &wc))
			client_on_completion(&wc);
	}

	return NULL;
}

void post_receives(struct connection* conn)
{
	struct ibv_recv_wr wr, * bad_wr = NULL;
	struct ibv_sge sge;

	wr.wr_id = (uintptr_t)conn;
	wr.next = NULL;
	wr.sg_list = &sge;
	wr.num_sge = 1;

	sge.addr = (uintptr_t)conn->recv_msg;
	sge.length = sizeof(struct message);
	sge.lkey = conn->recv_mr->lkey;

	TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));
}

void register_memory(struct connection* conn)
{
	conn->send_msg = malloc(sizeof(struct message));
	conn->recv_msg = malloc(sizeof(struct message));

	conn->rdma_local_region = malloc(RDMA_BUFFER_SIZE);
	conn->rdma_remote_region = malloc(RDMA_BUFFER_SIZE);
	memset(conn->rdma_local_region, 0, RDMA_BUFFER_SIZE);
	memset(conn->rdma_remote_region, 0, RDMA_BUFFER_SIZE);

	TEST_Z(conn->send_mr = ibv_reg_mr(
		s_ctx->pd,
		conn->send_msg,
		sizeof(struct message),
		0));

	TEST_Z(conn->recv_mr = ibv_reg_mr(
		s_ctx->pd,
		conn->recv_msg,
		sizeof(struct message),
		IBV_ACCESS_LOCAL_WRITE));

	TEST_Z(conn->rdma_local_mr = ibv_reg_mr(
		s_ctx->pd,
		conn->rdma_local_region,
		RDMA_BUFFER_SIZE,
		((s_mode == M_WRITE) ? 0 : IBV_ACCESS_LOCAL_WRITE)));

	TEST_Z(conn->rdma_remote_mr = ibv_reg_mr(
		s_ctx->pd,
		conn->rdma_remote_region,
		RDMA_BUFFER_SIZE,
		((s_mode == M_WRITE) ? (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE) : IBV_ACCESS_REMOTE_READ)));
}

void send_message(struct connection* conn)
{
	struct ibv_send_wr wr, * bad_wr = NULL;
	struct ibv_sge sge;

	memset(&wr, 0, sizeof(wr));

	wr.wr_id = (uintptr_t)conn;
	wr.opcode = IBV_WR_SEND;//在这种类型中，发送方并不知道数据会写到remote node的何处。接收方需要调用ibv_post_recv，并且将接收到的数据放到指定的地址中
	wr.sg_list = &sge;
	wr.num_sge = 1;
	wr.send_flags = IBV_SEND_SIGNALED;

	sge.addr = (uintptr_t)conn->send_msg;
	sge.length = sizeof(struct message);
	sge.lkey = conn->send_mr->lkey;

	while (!conn->connected);

	TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
}

void send_mr(void* context)
{
	struct connection* conn = (struct connection*)context;

	conn->send_msg->type = MSG_MR;
	memcpy(&conn->send_msg->data.mr, conn->rdma_remote_mr, sizeof(struct ibv_mr));

	send_message(conn);
}

void set_mode(enum mode m)
{
	s_mode = m;
}
