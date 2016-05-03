#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_channel.h>
#include "cJSON/cJSON.h"

static char * ngx_http_upm_parse(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static ngx_int_t ngx_http_upm_init_module(ngx_cycle_t *cycle);
static ngx_int_t ngx_http_upm_init_process(ngx_cycle_t *cycle);
static char * ngx_http_upm_init_main_conf(ngx_conf_t *cf, void *conf);
static void ngx_http_upm_channel_handler(ngx_event_t *ev);
static ngx_int_t ngx_http_upm_manager_handler(ngx_http_request_t *r);

typedef void (*ngx_http_upm_interface_handler_pt)(ngx_http_request_t *r);

static void ngx_http_upm_process_client_body(ngx_http_request_t *r);


typedef enum {
    UPM_PEER_DOWN = 0,
    UPM_PEER_UP,
    UPM_PEER_BACKUP,
    UPM_PEER_UPDATE,
    UPM_PEER_ADD,
    UPM_PEER_DEL,
    UPM_UPSTREAM_ADD,
    UPM_UPSTREAM_DEL,
} ngx_http_upm_action_t;

typedef struct {
    ngx_flag_t      enable;

    ngx_str_t       shm_name;
    ngx_int_t       shm_size;
    ngx_array_t    *dyupstreams; 
    ngx_shm_zone_t *shm_zone;
} ngx_http_upm_main_conf_t;

typedef struct {
    ngx_array_t                         *path_elts;//the URI delimitate by '/'
    ngx_http_upm_action_t                action;
    ngx_http_upm_interface_handler_pt   *pt;
} ngx_http_upm_ctx_t;

/****************************
 * adjust peer cmd type:
 * @set peer as backup;
 * @set peer disable traffic
 * @set peer enable traffic
 * @add peer
 * @del peer
 ****************************/

typedef struct {
    ngx_str_t   upstream_name;
    ngx_int_t   set_backup;
    ngx_str_t   addr;               //SOCKET addr --> IP:PORT;
} ngx_http_upm_set_peer_msg_t;

typedef struct {
    ngx_str_t   sockaddr;             //socket: "IP:PORT";
    ngx_int_t   weight;             //weight: 100;
    ngx_int_t   max_fails;          //max_failes: 1;
    ngx_int_t   fail_timeout;       //fail_timeout: 1;
} ngx_http_upm_peer_server_msg_t;

typedef struct {
    ngx_http_upm_action_t           type;

    ngx_str_t                       upstream_name;      //named upstream       
    ngx_array_t                    *servers;            //parsed ngx_http_upstream_server_t 
} ngx_http_upm_peer_msg_t;

/****************************
 * add upstream or del upstream
 * @add upstream block;
 * @del upstream block;
 ****************************/

ngx_socket_t   ngx_http_upm_socketpairs[NGX_MAX_PROCESSES][2];
ngx_socket_t  *worker_sockp;

static ngx_command_t ngx_http_upm_module_commands[] = {

    { ngx_string("upstream_manager"),
      NGX_HTTP_LOC_CONF|NGX_CONF_NOARGS,
      ngx_http_upm_parse,
      0,
      0,
      NULL },

    ngx_null_command
}

static ngx_http_module_t  ngx_http_upm_module_ctx = {
    ngx_http_upm_pre_conf,              /* preconfiguration */
    ngx_http_upm_init,                  /* postconfiguration */

    ngx_http_upm_create_main_conf,      /* create main configuration */
    ngx_http_upm_init_main_conf,        /* init main configuration */

    ngx_http_upm_create_srv_conf,       /* create server configuration */
    NULL,                               /* merge server configuration */

    NULL,                               /* create location configuration */
    NULL                                /* merge location configuration */
}

static ngx_module_t ngx_http_upm_module = {
    NGX_MODULE_V1,
    &ngx_http_upm_module_ctx,       /* module context */
    ngx_http_upm_commands,          /* module directives */
    NGX_HTTP_MODULE,                /* module type */
    NULL,                           /* init master */
    ngx_http_upm_init_module,       /* init module */
    ngx_http_upm_init_process,      /* init process */
    NULL,                           /* init thread */
    NULL,                           /* exit thread */
    ngx_http_upm_exit_process,      /* exit process */
    NULL,                           /* exit master */
    NGX_MODULE_V1_PADDING
}


static ngx_int_t 
ngx_http_upm_init_module(ngx_cycle_t *cycle)
{
    ngx_int_t i, on;
    char *name = "ngx_http_upm_init_module";
    ngx_core_conf_t *ccf = (ngx_core_conf_t *) ngx_get_conf(cycle->conf_ctx, ngx_core_module);
    ngx_int_t workers = ccf->worker_processes;
    
    /*
     *We need initilize all sync channel fd;
     *Currently, ngx_last_process must be zero, because we haven't fork any child;
     * */
    for (i = 0; i < workers; i++) {
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, ngx_http_upm_socketpairs[i]) == -1)
        {
            ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno,
                          "socketpair() failed while do \"%s\"", name);
            return NGX_ERROR;
        }

        ngx_log_debug2(NGX_LOG_DEBUG_CORE, cycle->log, 0,
                       "channel %d:%d",
                       ngx_http_upm_socketpairs[i][0],
                       ngx_http_upm_socketpairs[i][1]);

        if (ngx_nonblocking(ngx_http_upm_socketpairs[i][0]) == -1) {
            ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno,
                          ngx_nonblocking_n " failed while do \"%s\"",
                          name);
            ngx_close_channel(ngx_http_upm_socketpairs[i], cycle->log);
            return NGX_ERROR;
        }

        if (ngx_nonblocking(ngx_http_upm_socketpairs[i][1]) == -1) {
            ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno,
                          ngx_nonblocking_n " failed while do \"%s\"",
                          name);
            ngx_close_channel(ngx_processes[s].channel, cycle->log);
            return NGX_ERROR;
        }

        on = 1;
        if (ioctl(ngx_http_upm_socketpairs[i][0], FIOASYNC, &on) == -1) {
            ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno,
                          "ioctl(FIOASYNC) failed while do \"%s\"", name);
            ngx_close_channel(ngx_http_upm_socketpairs[i], cycle->log);
            return NGX_ERROR;
        }

        if (fcntl(ngx_http_upm_socketpairs[i][0], F_SETOWN, ngx_pid) == -1) {
            ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno,
                          "fcntl(F_SETOWN) failed while do \"%s\"", name);
            ngx_close_channel(ngx_http_upm_socketpairs[i], cycle->log);
            return NGX_ERROR;
        }

        if (fcntl(ngx_http_upm_socketpairs[i][0], F_SETFD, FD_CLOEXEC) == -1) {
            ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno,
                          "fcntl(FD_CLOEXEC) failed while do \"%s\"",
                           name);
            ngx_close_channel(ngx_http_upm_socketpairs[i], cycle->log);
            return NGX_ERROR;
        }

        if (fcntl(ngx_http_upm_socketpairs[i][1], F_SETFD, FD_CLOEXEC) == -1) {
            ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno,
                          "fcntl(FD_CLOEXEC) failed while do \"%s\"",
                           name);
            ngx_close_channel(ngx_http_upm_socketpairs[i], cycle->log);
            return NGX_ERROR;
        }
    }
}

static char * 
ngx_http_upm_init_main_conf(ngx_conf_t *cf, void *conf)
{
    ngx_http_upm_main_conf_t *ummcf = conf;

    ummcf->shm_zone = ngx_shared_memory_add(cf, &ummcf->shm_name, 
                                            ummcf->shm_size, &ngx_http_upm_module);
    if (ummcf->shm_zone == NULL) {
        ngx_conf_log_error(NGX_LOG_WARN, cf, 0,
                                   "create shared memory failed");
        return NGX_CONF_ERROR;
    }

    return NGX_CONF_OK;
}


static char * 
ngx_http_upm_parse(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) 
{
    // @cmd format: 
    //      upstream_manager shm_size=20M shm_name=upm1;
    ssize_t                      shm_size;
    ngx_str_t                   *value;
    ngx_str_t                   *shm_name;

    ngx_http_core_loc_conf_t    *clcf;
    ngx_http_upm_main_conf_t    *ummcf;            

    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    ummcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_upm_module);

    value = cf->args->elts;
    if (cf->args->nelts != 3) {
        ngx_conf_log_error(NGX_LOG_WARN, cf, 0,
                                   "configed bad args count");
        return NGX_CONF_ERROR;
    }

    if (ngx_strncmp((value[1].data + 9), "shm_size", 9) ||
        ngx_strncmp((value[2].data + 9), "shm_name", 9) ) {
        ngx_conf_log_error(NGX_LOG_WARN, cf, 0,
                                   "configed bad format args");
        return NGX_CONF_ERROR;
    }

    shm_size = ngx_parse_size(value[1].data + 9);
    if (shm_size == NGX_ERROR) {
        ngx_conf_log_error(NGX_LOG_WARN, cf, 0,
                                   "parse shm_size failed");
        return NGX_CONF_ERROR;
    }

    shm_name.data = value[2].data + 9;
    shm_name.len = value[2].len - 9;

    ummcf->shm_size = shm_size;
    ummcf->shm_name = shm_name;

    clcf->content_handler = ngx_http_upm_manger_handler;
    ummcf->enable = 1;
    return NGX_CONF_OK;
}


static ngx_int_t 
ngx_http_upm_init_process(ngx_cycle_t *cycle)
{
    //Get the worker process private socket pair;
    //worker spawn index ==> ngx_worker
    ngx_int_t       n;

    worker_sockp = ngx_http_upm_socketpairs[ngx_worker];
    //@The really used channel:
    //      ngx_http_upm_socketpairs[i][0]
    //@Close the nounsed channel:
    //      Here, we close the socketpair[1]
    for (n = 0; n < ngx_last_process; n++) {
        //ngx_process_slot is also the process_list index;
        if (n == ngx_worker) {
            continue;
        }

        if (ngx_http_upm_socketpairs[n][1] == -1) {
            continue;
        }

        if (close(ngx_http_upm_socketpairs[n][1]) == -1) {
            ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno,
                          "close() ngx_http_upm_module channel failed");
            return NGX_ERROR;
        }
    }

    if (ngx_add_channel_event(cycle, ngx_http_upm_socketpairs[ngx_worker][0], 
                              NGX_READ_EVENT, 
                              ngx_http_upm_channel_handler) == NGX_ERROR) {
        ngx_log_error(NGX_LOG_ALERT, cycle->log, 
                      ngx_errno, "failed to register channel handler while init upm_module worker");
        return NGX_ERROR;
    }
    return NGX_OK;    
}

static void 
ngx_http_upm_channel_handler (ngx_event_t *ev)
{
    ngx_channel_t       *ch;
    ngx_connection_t    *c;




}

static ngx_array_t *
ngx_upm_parse_path(ngx_pool_t *pool, ngx_str_t *s) 
{
    char            *p, *q, *e;
    ngx_str_t       *r;
    ngx_array_t     *res;

    enum {
        sw_ps = 0,
        sw_pm,
    } state;

    res = ngx_array_create(pool, 8, sizeof(ngx_str_t));
    if (res == NULL) {
        ngx_log_error(NGX_LOG_ALERT, pool->log, 0,
                          "ngx_upm_parse_path allocate failed");
        return NULL 
    }
    p = s->data;
    q = s->data;

    e = s->data + s->len;
    state st = sw_ps;
    for(p = s->data; p < e; p++) {
        if (*p == '/' && st == sw_ps) {
            q = ++p;
            st = sw_pm;
        } else if (( *p == '/' && st == sw_pm) || ( p + 1 == e)) {
            r = ngx_array_push(res);
            r->data = q;
            r->len = p - q;

            if ( p + 1 < e) {
                st = sw_pm;
                q = p + 1; //point to next part start
            }
        }
    }
}


static ngx_buf_t *
ngx_http_upm_read_body(ngx_http_request_t *r)
{
    size_t        len;
    ngx_buf_t    *buf, *next, *body;
    ngx_chain_t  *cl;

    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "[upm] read post body");

    cl = r->request_body->bufs;
    buf = cl->buf;

    if (cl->next == NULL) {
        return buf;

    } else {

        //memory buf only have two elts;
        next = cl->next->buf;
        len = (buf->last - buf->pos) + (next->last - next->pos);

        body = ngx_create_temp_buf(r->pool, len);
        if (body == NULL) {
            return NULL;
        }

        body->last = ngx_cpymem(body->last, buf->pos, buf->last - buf->pos);
        body->last = ngx_cpymem(body->last, next->pos, next->last - next->pos);
    }

    return body;
}

static ngx_buf_t *
ngx_http_upm_read_body_from_file(ngx_http_request_t *r)
{
    size_t        len;
    ssize_t       size;
    ngx_buf_t    *buf, *body;
    ngx_chain_t  *cl;

    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "[upm] read post body from file");

    len = 0;
    cl = r->request_body->bufs;

    //collect all bufs to a single big buffer;
    while (cl) {

        buf = cl->buf;

        if (buf->in_file) {
            len += buf->file_last - buf->file_pos;

        } else {
            len += buf->last - buf->pos;
        }

        cl = cl->next;
    }

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "[upm] read post body file size %ui", len);

    body = ngx_create_temp_buf(r->pool, len);
    if (body == NULL) {
        return NULL;
    }

    cl = r->request_body->bufs;

    while (cl) {

        buf = cl->buf;
        if (buf->in_file) {
            size = ngx_read_file(buf->file, body->last,
                                 buf->file_last - buf->file_pos, buf->file_pos);

            if (size == NGX_ERROR) {
                return NULL;
            }

            body->last += size;

        } else {

            body->last = ngx_cpymem(body->last, buf->pos, buf->last - buf->pos);
        }

        cl = cl->next;
    }

    return body;
}



/* @uri format:
 *
 *@add peer:
 *      /upstream/manager/peer/add/
 *@del peer:
 *      /upstream/manager/peer/del/
 *@set peer down:
 *      /upstream/manager/peer/down/ 
 *@set peer up:
 *      /upstream/manager/peer/up/
 *@adjust peer arg:
 *      /upstream/manager/peer/adjust/
 **/

static ngx_int_t 
ngx_http_upm_manager_handler(ngx_http_request_t *r)
{
    ngx_int_t                           rc; 
    ngx_str_t                           *elts;
    ngx_array_t                         *res;
    ngx_http_upm_ctx_t                  *ctx;
    ngx_http_upstream_t                 *u;
    ngx_http_upm_main_conf_t            *plcf;

    //Only support POST method;
    if (r->method != NGX_HTTP_POST) {
        return NGX_HTTP_NOT_ALLOWED;
    }

    ctx = ngx_pcalloc(r->pool, sizeof(ngx_http_upm_ctx_t));
    if (ctx == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    res = ngx_upm_parse_path(r->pool, &r->uri);
    if (res == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    ctx->path_elts = res;
    elts = res->elts;

    if (res->nelts <= 3 ) {
        ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno,
                          "socketpair() failed while do \"%s\"", name);
        return NGX_HTTP_NOT_FOUND;
    }
    //Check interface format:
    if (ngx_strncmp(elts[0].data, "upstream", 8) == 0 && 
        ngx_strncmp(elts[1].data, "manager", 7) == 0 && 
        ngx_strncmp(elts[2].data, "peer", 4) == 0 ) 
    {
        // @add peer:
        if (ngx_strncmp(elts[3].data, "add", 3)) {
            ctx->action = UPM_PEER_ADD;
            ctx->pt = ngx_http_upm_add_handler;
        } else if (ngx_strncmp(elts[3].data, "del", 3)) {
            ctx->action = UPM_PEER_DEL;
            ctx->pt = ngx_http_upm_del_handler;
        } else if (ngx_strncmp(elts[3].data, "down", 4)) {
            ctx->action = UPM_PEER_DOWN;
            ctx->pt = ngx_http_upm_down_handler;
        } else if (ngx_strncmp(elts[3].data, "up", 2)) {
            ctx->action = UPM_PEER_UP;
            ctx->pt = ngx_http_upm_up_handler;
        } else if (ngx_strncmp(elts[3].data, "backup")) {
            ctx->action = UPM_PEER_BACKUP;
            ctx->pt = ngx_http_upm_backup_handler;
        } else if (ngx_strncmp(elts[3].data, "update")) {
            ctx->action = UPM_PEER_UPDATE;
            ctx->pt = ngx_http_upm_update_handler;
        } else {
            return NGX_HTTP_NOT_FOUND;
        }
    } else {
        return NGX_HTTP_NOT_FOUND;
    }
    ngx_http_read_client_request_body(r, ngx_http_upm_process_client_body);
}

static void 
ngx_http_upm_process_client_body(ngx_http_request_t *r)
{
    char                        *upname;
    cJSON                       *root, *servers;
    ngx_str_t                   *value, rv;
    ngx_int_t                    status;
    ngx_buf_t                   *body;
    ngx_array_t                 *path_elts;
    ngx_http_upm_ctx_t          *ctx;

    ngx_str_set(&rv, "");

    ctx = ngx_http_get_module_ctx(r, ngx_http_upm_module);
    if (ctx == NULL) {
        ngx_log_error(NGX_LOG_ERROR, r->connection->log, 0,
                  "[upm] get ngx_http_upm_module ctx faild");
        ngx_str_set(&rv, "")
        status = NGX_HTTP_INTERNAL_SERVER_ERROR;
        goto finish;
    }
    
    path_elts = ctx->path_elts;
    if (r->request_body == NULL || r->request_body->bufs == NULL) {
        status = NGX_HTTP_NO_CONTENT;
        ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                       "[upm] no content");
        ngx_str_set(&rv, "no content\n");
        goto finish;
    }

    if (r->request_body->temp_file) {
        body = ngx_http_upm_read_body_from_file(r);
    } else {
        body = ngx_http_upm_read_body(r);
    }

    if (body == NULL) {
        status = NGX_HTTP_INTERNAL_SERVER_ERROR;
        ngx_str_set(&rv, "out of memory\n");
        goto finish;
    }

    /*
     * start to parse the JSON body, currently only support adjust peer methods;
     */
    
    jsonroot = cJSON_Parse(body->start);
    upname = cJSON_GetObjectItem(jsonroot, "upstream_name")->valuestring;
    if (upname == NULL) {
        ngx_str_set(&rv, "upstream_name can't be null");
        status = NGX_HTTP_NOT_FOUND;
        goto finished;
    }

    jsonservers = cJSON_GetObjectItem(jsonroot, "servers"); 
    if (jsonservers == NULL) {
        ngx_str_set(&rv, "upstream servers can't be null");
        status = NGX_HTTP_NOT_FOUND;
        goto finished;
    }   

    servcount = cJSON_GetArraySize(jsonservers);
    if (servers == NULL) {
        ngx_str_set(&rv, "upstream servers count must big than 0");
        status = NGX_HTTP_NOT_FOUND;
        goto finished;
    }   

    ngx_http_upm_peer_msg_t *msg = ngx_pcalloc(r->pool, sizeof(ngx_http_upm_peer_msg_t));
    if (msg == NULL) {
        status = NGX_HTTP_INTERNAL_SERVER_ERROR;
        goto finished;
    }

    msg->servers = ngx_array_create(r->pool, 16, sizeof(ngx_http_upm_peer_server_msg_t));
    if (msg->servers == NULL) {
        status = NGX_HTTP_INTERNAL_SERVER_ERROR;
        goto finished;
    }

    ngx_str_t *sockaddr;
    ngx_int_t  weight, max_fails, fail_timeout;
    weight = -1;
    max_fails = -1;
    fail_timeout = -1;
    cJSON *serv = jsonservers->child;
    ngx_http_upm_peer_server_msg_t *serv_msg;
    while ( serv != NULL){
        //get the server socket address;
        sockaddr = cJSON_GetObjectItem(serv, "socket")->valuestring; 
        if (sockaddr == NULL) {
            ngx_str_set(&rv, "server sockaddr can't be null");
            status = NGX_HTTP_NOT_FOUND;
            goto finished;
        }
        
        weight = cJSON_GetObjectItem(serv, "weight")->valueint;
        max_fails = cJSON_GetObjectItem(serv, "max_failes")->valueint;
        fail_timeout = cJSON_GetObjectItem(serv, "fail_timeout")->valueint;

        serv_msg = ngx_array_push(msg->servers);
        if (serv_msg == NULL) {
            status = NGX_HTTP_INTERNAL_SERVER_ERROR;
            goto finished;
        }
        serv_msg->sockaddr.data = sockaddr;
        serv_msg->sockaddr.len = ngx_strlen(sockaddr);
        
        serv_msg->weight = weight;
        serv_msg->max_fails = max_fails;
        serv_msg->fail_timeout = fail_timeout;
    }
    
    ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                  "[dyups] post upstream name: %V", &name);

    status = ngx_dyups_update_upstream(&name, body, &rv);

finish:

    ngx_http_dyups_send_response(r, status, &rv);
}

