#include "spdk/stdinc.h"

#include "spdk/bdev.h"
#include "spdk/env.h"
#include "spdk/event.h"
#include "spdk/blob_bdev.h"
#include "spdk/blob.h"
#include "spdk/log.h"
#include "spdk/string.h"

#include "spdk/blobfs.h"

uint32_t gcore = 0;
bool blobfs_ready = false;
bool gerror = false;
bool blobfs_shutdown = false;
struct spdk_poller* g_poller = NULL;

struct hello_context_t{
    struct spdk_filesystem* fs;
    struct spdk_file* file;
    struct spdk_fs_thread_ctx* ctx;
    uint64_t io_unit_size;
    uint8_t* read_buff;
    uint8_t* write_buff;
    int rc;
};

struct hello_context_t* g_ctx = NULL;

static void hello_cleanup(struct hello_context_t * hello_context){
    spdk_free(hello_context->read_buff);
    spdk_free(hello_context->write_buff);
    free(hello_context);
    SPDK_NOTICELOG("free context done\n");
}

static void unload_complete(void* cb_arg, int fserrno){
    SPDK_NOTICELOG("entry unload complete\n");

}

static void unload_blobfs(struct hello_context_t* hello_context){
    spdk_fs_unload(hello_context->fs, unload_complete, hello_context);
}

static void delete_file(struct hello_context_t* hello_context){
    int rc = spdk_fs_delete_file(hello_context->fs, hello_context->ctx, "file1");
    if (rc != 0){
        SPDK_ERRLOG("fail to delete file\n");
    }else{
        SPDK_NOTICELOG("delete file success\n");
    }

    blobfs_shutdown = true;
}

static void close_file(struct hello_context_t* hello_context){
    int rc = spdk_file_close(hello_context->file, hello_context->ctx);
    if (rc!=0){
        SPDK_ERRLOG("fail to close file\n");
    }else{
        SPDK_NOTICELOG("close file success\n");
    }

    delete_file(hello_context);
}

static void check_data(struct hello_context_t* hello_context){
    int match_res = -1;
    match_res = memcmp(hello_context->write_buff, hello_context->read_buff, 
        hello_context->io_unit_size);
    
    if (match_res){
        SPDK_ERRLOG("data compare fail\n");
    } else {
        SPDK_NOTICELOG("read SUCCESS and data matches!\n");
    }

    close_file(hello_context);
}

static void read_file(struct hello_context_t* hello_context){
    
    hello_context->read_buff = spdk_malloc(hello_context->io_unit_size,
        0x1000, NULL, SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
    if (hello_context->read_buff == NULL){
        SPDK_ERRLOG("fail to allocate read buffer\n");
        return;
    }

    int rc = spdk_file_read(hello_context->file, hello_context->ctx, 
        hello_context->read_buff, 0, hello_context->io_unit_size);
    if (rc >= 0){
        SPDK_NOTICELOG("read data success\n");
    }else{
        SPDK_ERRLOG("fail to read data\n");
    }

    check_data(hello_context);
}

static void write_file(struct hello_context_t* hello_context){
    hello_context -> write_buff = spdk_malloc(hello_context->io_unit_size,
        0x1000, NULL, SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
    if(hello_context->write_buff == NULL){
        SPDK_ERRLOG("fail to allocate DMA buffer\n");
    }

    memset(hello_context->write_buff, 0x5a, hello_context->io_unit_size);

    int rc = spdk_file_write(hello_context->file, hello_context->ctx, 
        hello_context->write_buff, 0, hello_context->io_unit_size);
    if(rc==0){
        SPDK_NOTICELOG("write data success\n");
    }else{
        SPDK_ERRLOG("fail to write data\n");
    }

    read_file(hello_context);
}

static void create_file(struct hello_context_t* hello_context){
    hello_context->ctx = spdk_fs_alloc_thread_ctx(hello_context->fs);

    int rc = spdk_fs_create_file(hello_context->fs, hello_context->ctx,
        "file1");
    if (rc!=0){
        SPDK_NOTICELOG("fail to create file1!!\n");
    }else{
        SPDK_NOTICELOG("create file success\n");
    }

    rc = spdk_fs_open_file(hello_context->fs, hello_context->ctx, "file1", 
        0, &hello_context->file);
    if (rc!=0){
        SPDK_ERRLOG("fail to open file1!!\n");
    }else{
        SPDK_NOTICELOG("open file success\n");
    }

    write_file(hello_context);
}

static void base_bdev_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
		   void *event_ctx){
	SPDK_WARNLOG("Unsupported bdev event: type %d\n", type);
}

int poller_fn(void* arg){
    if (blobfs_shutdown){
        SPDK_NOTICELOG("shutdown signal recieved\n");
        unload_blobfs(g_ctx);
        spdk_poller_unregister(&g_poller);
        spdk_app_stop(0);
    }
    return SPDK_POLLER_BUSY;
}

static void fs_load_cb(void *ctx, struct spdk_filesystem *fs, int fserrno){
    struct hello_context_t * hello_context = ctx;
	if (fserrno == 0) {
		hello_context->fs = fs;
        SPDK_NOTICELOG("start fs success\n");
	}

    hello_context->io_unit_size = 512;

    // register a shutdown poller
    g_poller = spdk_poller_register(poller_fn, NULL, 0);
    SPDK_NOTICELOG("poller registered\n");

    blobfs_ready = true;

    g_ctx = hello_context;
}

static void __call_fn(void *arg1, void *arg2){
	fs_request_fn fn;

	fn = (fs_request_fn)arg1;
	fn(arg2);
}

static void
__send_request(fs_request_fn fn, void *arg)
{
	struct spdk_event *event;

	event = spdk_event_allocate(gcore, __call_fn, (void *)fn, arg);
	spdk_event_call(event);
}

static void hello_start(void* arg1){
    struct hello_context_t *hello_context = arg1;
	struct spdk_bs_dev *bs_dev = NULL;
	int rc;

    rc = spdk_bdev_create_bs_dev_ext("Malloc0", base_bdev_event_cb, NULL, &bs_dev);
	if (rc != 0) {
        gerror = true;
		SPDK_ERRLOG("Could not create blob bdev, %s!!\n",
			    spdk_strerror(-rc));
		spdk_app_stop(-1);
		return;
	}

    gcore = spdk_env_get_first_core();

    struct spdk_blobfs_opts* opts = calloc(1, sizeof(struct spdk_blobfs_opts));

    spdk_fs_opts_init(opts);
    
    spdk_fs_init(bs_dev, opts, __send_request, fs_load_cb, hello_context);

}

static void *initialize_spdk(void* arg){

    struct spdk_app_opts opts = {};
	struct hello_context_t *hello_context = NULL;

	spdk_app_opts_init(&opts, sizeof(opts));
    opts.name = "hello_blobfs";
	opts.json_config_file = arg;

    hello_context = calloc(1, sizeof(struct hello_context_t));

    opts.reactor_mask = "0x3";

    if (hello_context != NULL){
        SPDK_NOTICELOG("start app\n");

        int rc = spdk_app_start(&opts, hello_start, hello_context);
        if (rc) {
            gerror = true;
            SPDK_ERRLOG("ERROR\n");
            spdk_app_fini();
            return NULL;
        }else{
            SPDK_NOTICELOG("SUCCESS\n");
        }
        hello_cleanup(hello_context);
    }else{
        SPDK_ERRLOG("fail to alloc hello_context!\n");
    }

    spdk_app_fini();
    return NULL;
}


int main(int argc, char** argv){
    int rc = 0;

    pthread_t init_thread;

    pthread_create(&init_thread, NULL, &initialize_spdk, argv[1]);

    while (!blobfs_ready && !gerror) {
        sleep(1);
    }

    if (gerror){
        SPDK_ERRLOG("global error is true\n");
        return rc;
    }

    create_file(g_ctx);

    pthread_join(init_thread, NULL);

    return rc;
}
