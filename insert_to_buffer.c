//
//  insert_to_buffer.c
//  RAB
//
//  Created by Lin on 2021/12/3.
//

#include "insert_to_buffer.h"

struct ssd_info *buffer_management(struct ssd_info *ssd)//----ver1:lpnFlag
{
    unsigned int lsn,lpn,last_lpn,first_lpn,offset;
    struct request *new_request;
    struct buffer_group *buffer_node,key;
    struct sub_request *sub=NULL;

    ssd->dram->current_time=ssd->current_time;//设置缓存dram的当前时间为系统的当前时间
    new_request=ssd->request_tail; //从队尾开始处理
    lsn=new_request->lsn;           //请求的起始逻辑扇区号
    lpn=new_request->lsn/ssd->parameter->subpage_page;
    last_lpn=(new_request->lsn+new_request->size-1)/ssd->parameter->subpage_page;//last_lpn即该IO请求项操作的最大目标页号
    first_lpn=new_request->lsn/ssd->parameter->subpage_page;//first_lpn即该IO请求项操作的起始目标页号
    
    //ssd->all_page_count += (last_lpn-first_lpn+1);用于统计---hit_ratio

    
    if(new_request->operation==READ)
    {
        while(lpn<=last_lpn)//当前lpn小于或等于最大目标页号last_lpn时，也就是说明该IO读请求操作尚未完成
        {
            int nextLpn = lpn;
            lpn = lpn % ssd->stripe.checkStart;
            lpn += 1;//已改---防止lpn=0，所以+1---防止3672出错
            offset = get_offset(ssd, new_request,lpn);
            key.group=lpn;
            buffer_node = (struct buffer_group *) hash_find(ssd->dram->databuffer, (HASH_NODE *) &key);
            if(buffer_node==NULL)
            {
                ssd->dram->map->map_entry[lpn].lpnFlag=1;
                //creat_sub_request(ssd,lpn,size(offset),offset,new_request,new_request->operation,0,ssd->page2Trip[lpn]);//已改--CJ
                //creat_sub_request(ssd,lpn,sub_size,sub_state,new_request,new_request->operation,0, ssd->page2Trip[lpn]);//---sha
                ssd->dram->databuffer->read_miss_hit++;
            }
            else
            {
                if((buffer_node->stored & offset)==offset)//读-全命中---提到head
                {
                    ssd->dram->databuffer->read_hit++;
                    Set_First(ssd, buffer_node);
                }
                else//---部分命中
                {
                    ssd->dram->map->map_entry[lpn].lpnFlag=1;
                    //creat_sub_request(ssd,lpn,size(offset),offset,new_request,new_request->operation,0,ssd->page2Trip[lpn]);
                    //creat_sub_request(ssd,lpn,sub_size,sub_state,new_request,new_request->operation,0, ssd->page2Trip[lpn]);
                    ssd->dram->databuffer->read_miss_hit++;
                }
            }
            lpn = ++nextLpn;
            //lpn++;
        }
    }
    else if(new_request->operation==WRITE)
    {
        while(lpn<=last_lpn)
        {
            key.group=lpn;
            int nextLpn = lpn;
            lpn = lpn % ssd->stripe.checkStart;
            lpn += 1;
            buffer_node = (struct buffer_group *) hash_find(ssd->dram->databuffer, (HASH_NODE *) &key);
            offset = get_offset(ssd, new_request, lpn);
            if(buffer_node!=NULL)
            {
                buffer_node->stored = buffer_node->stored|offset;
                buffer_node->dirty_clean=buffer_node->dirty_clean|offset;//?
                ssd->dram->databuffer->write_hit++;
                Set_First(ssd, buffer_node);
            }
            else{
                ssd->dram->map->map_entry[lpn].lpnFlag=1;//已改---置为1
                insert2buffer(ssd,lpn,NULL,new_request);
            }
            lpn = ++nextLpn;
        }
    }

    if(new_request->subs==NULL)
    {
        new_request->begin_time=ssd->current_time;
        new_request->response_time=ssd->current_time+1000;
    }

    return ssd;
}

int find_node_READ(struct  ssd_info *ssd,unsigned int lpn,struct request *req){
    unsigned int offset;
    struct buffer_group *buffer_node,key;

    offset = get_offset(ssd, req,lpn);
    key.group=lpn;
    buffer_node = (struct buffer_group *) hash_find(ssd->dram->databuffer, (HASH_NODE *) &key);
    if(buffer_node==NULL)
    {
        ssd->dram->databuffer->read_miss_hit++;
        return 0;
    }
    else
    {
        if((buffer_node->stored & offset)==offset)//读-全命中---提到head
        {
            ssd->dram->databuffer->read_hit++;
            Set_First(ssd, buffer_node);
            return 1;
        }
        else//---部分命中
        {
            ssd->dram->databuffer->read_miss_hit++;
            return 0;
        }
    }

}

void find_node(struct  ssd_info *ssd,unsigned int lpn,struct request *req){
    unsigned int offset;
    struct buffer_group *buffer_node,key;

    key.group=lpn;
    buffer_node = (struct buffer_group *) hash_find(ssd->dram->databuffer, (HASH_NODE *) &key);
    offset = get_offset(ssd,req, lpn);
    if(buffer_node!=NULL){
     buffer_node->stored = buffer_node->stored|offset;
     buffer_node->dirty_clean=buffer_node->dirty_clean|offset;
     ssd->dram->databuffer->write_hit++;
     Set_First(ssd,buffer_node);
     //return NULL;
    }
    else{
        insert2buffer(ssd,lpn,NULL,req);//对写，sub传空即可
        ssd->dram->databuffer->write_miss_hit++;
        //return 0;
    }

}



struct ssd_info * insert2buffer(struct ssd_info *ssd,unsigned int lpn,struct sub_request *sub,struct request *req)//参数sub的作用是针对读请求往缓存写--部分命中的情况
{
    struct buffer_group *buffer_node, *new_node,*temp,key;
    unsigned int sub_req_state = 0, sub_req_size = 0, sub_req_lpn = 0;
    unsigned int last_lpn, first_lpn, state;
    struct sub_request *sub1 = NULL;
    unsigned int mask = 0;
    unsigned int offset1 = 0, offset2 = 0;


    last_lpn = (req->lsn + req->size - 1) / ssd->parameter->subpage_page;
    first_lpn = req->lsn / ssd->parameter->subpage_page;


    key.group=lpn;
    temp = (struct buffer_group *) hash_find(ssd->dram->databuffer, (HASH_NODE *) &key);
    if(temp!=NULL){ //---针对读部分命中:更新状态，并提到队头
        temp->stored=temp->stored | sub->state;
        Set_First(ssd, temp);
        return ssd;   //针对读部分命中---更新状态+置为队头---完毕后返回
    }
    if(ssd->dram->databuffer->current_buffer_page >= ssd->dram->databuffer->max_buffer_page)
    {
        if (ssd->parameter->subpage_page == 32) {
            mask = 0xffffffff;
        } else {
            mask = ~(0xffffffff << (ssd->parameter->subpage_page));
        }
        state = mask;

        if (lpn == first_lpn) {
            offset1 = ssd->parameter->subpage_page - ((lpn + 1) * ssd->parameter->subpage_page - req->lsn);
            state = state & (0xffffffff << offset1);
        }
        if (lpn == last_lpn) {
            offset2 = ssd->parameter->subpage_page -
                      ((lpn + 1) * ssd->parameter->subpage_page - (req->lsn + req->size));
            if (offset2 != 32) {
                state = state & (~(0xffffffff << offset2));
            }
        }

        buffer_node = ssd->dram->databuffer->buffer_tail;
        
        ssd->dram->databuffer->buffer_tail = ssd->dram->databuffer->buffer_tail->LRU_link_pre;
        ssd->dram->databuffer->buffer_tail->LRU_link_next = NULL;

        //ssd->dram->databuffer->write_miss_hit++;
        //ssd->pop_count++;用于统计
        if(buffer_node->dirty_clean>0){//脏的才弹，干净的不弹--直接删除结点
            sub_req_state = buffer_node->stored;
            sub_req_size = size(buffer_node->stored);
            sub_req_lpn = buffer_node->group;
            if(req->operation == READ){
                creat_sub_request(ssd,buffer_node->group,size(buffer_node->stored),buffer_node->stored,req,WRITE,0,ssd->page2Trip[sub_req_lpn]);//将尾结点这个请求挂到请求队列，结点弹下去后，新读结点才能进来，方便计算时间
            }else{
                if (ssd->dram->map->map_entry[sub_req_lpn].state == 0 &&
                    ssd->parameter->allocation_scheme == 0 &&
                    ssd->parameter->dynamic_allocation == 2) {//第一次写
                    //creat_sub_write_request_for_raid(ssd, lpn, state, req, mask);
                    creat_sub_write_request_for_raid(ssd, sub_req_lpn, sub_req_state, req,
                                                     ~(0xffffffff
                                                             << (ssd->parameter->subpage_page)));//此为第一次组条带的过程
                } else {
                    sub1=creat_sub_request(ssd, sub_req_lpn, sub_req_size, sub_req_state, req,
                                           WRITE, 0,
                                           ssd->page2Trip[sub_req_lpn]);//脏数据就写下去----更新写
                    ppc_cache(ssd, ssd->page2Trip[sub_req_lpn], sub_req_state, req, mask, sub_req_lpn, sub1);
                }
            }
        }

        ssd->dram->databuffer->current_buffer_page--;
        buffer_node->LRU_link_pre = buffer_node->LRU_link_next = NULL;
        hash_del(ssd->dram->databuffer, (HASH_NODE *) buffer_node);
        hash_node_free(ssd->dram->databuffer, (HASH_NODE *) buffer_node);

        buffer_node = NULL;
    }
    
    new_node=NULL;
    new_node=(struct buffer_group *)malloc(sizeof(struct buffer_group));
    alloc_assert(new_node,"buffer_group_node");
    memset(new_node,0, sizeof(struct buffer_group));
        
    new_node->group = lpn;//把该lpn设置为缓存新节点
    if(req->operation==READ){
        new_node->stored=sub->state;//什么意思？
    }else{
        new_node->stored = get_offset(ssd, req, lpn);//判断扇区是不是在缓存里面，1在，0不在
    }
    if(req->operation == WRITE){
        new_node->dirty_clean=new_node->stored;//什么意思？
    }else{
        new_node->dirty_clean=0;
    }

    new_node->LRU_link_pre = NULL;
    new_node->LRU_link_next = NULL;
    if(ssd->dram->databuffer->buffer_head == NULL)
        ssd->dram->databuffer->buffer_head = ssd->dram->databuffer->buffer_tail = new_node;
    else
    {
        new_node->LRU_link_next = ssd->dram->databuffer->buffer_head;
        ssd->dram->databuffer->buffer_head->LRU_link_pre = new_node;
        ssd->dram->databuffer->buffer_head = new_node;
    }
    hash_add(ssd->dram->databuffer, (HASH_NODE *) new_node);
    ssd->dram->databuffer->current_buffer_page++;
    return ssd;
}


unsigned int get_offset(struct ssd_info *ssd, struct request *req, unsigned int lpn)
{
    unsigned long first_lsn = req->lsn;
    unsigned long last_lsn = req->lsn + req->size - 1;
    
    unsigned long first_lpn = first_lsn/ssd->parameter->subpage_page;
    unsigned long last_lpn = last_lsn/ssd->parameter->subpage_page;
    
    unsigned int offset = 0;
    unsigned int mask = (1<<ssd->parameter->subpage_page) - 1;
    
    if(first_lpn == last_lpn)
    {
        unsigned long sub_page = last_lsn - first_lsn + 1;
        offset = ~(0xffffffff<<sub_page);
        offset = offset<<(first_lsn%ssd->parameter->subpage_page);
    }
    else if(lpn == first_lpn)
    {
        offset = 0xffffffff<<(first_lsn%ssd->parameter->subpage_page);
        offset = offset & mask;
    }
    else if(lpn == last_lpn)
    {
        offset = 0xffffffff<<(last_lsn%ssd->parameter->subpage_page+1);
        offset = ~offset;
    }
    else
    {
        offset = mask;
    }

    return offset;
}

void Set_First(struct ssd_info *ssd,struct buffer_group *buffer_node)
{
    if(ssd->dram->databuffer->buffer_head!=buffer_node)
    {
        if(ssd->dram->databuffer->buffer_tail==buffer_node)
        {
            buffer_node->LRU_link_pre->LRU_link_next=NULL;
            ssd->dram->databuffer->buffer_tail=buffer_node->LRU_link_pre;
        }
        else
        {
            buffer_node->LRU_link_pre->LRU_link_next=buffer_node->LRU_link_next;
            buffer_node->LRU_link_next->LRU_link_pre=buffer_node->LRU_link_pre;
        }
        
        buffer_node->LRU_link_next=ssd->dram->databuffer->buffer_head;
        ssd->dram->databuffer->buffer_head->LRU_link_pre=buffer_node;
        buffer_node->LRU_link_pre=NULL;
        ssd->dram->databuffer->buffer_head=buffer_node;
        //ssd->node_move++;
    }
}

//void Set_First_warm_up(struct ssd_info *ssd, struct buffer_group *buffer_node)
//{
//    if (ssd->dram->buffer->buffer_head != buffer_node)
//    {
//        if (ssd->dram->buffer->buffer_tail == buffer_node)
//        {
//            buffer_node->LRU_link_pre->LRU_link_next = NULL;
//            ssd->dram->buffer->buffer_tail = buffer_node->LRU_link_pre;
//        }
//        else
//        {
//            buffer_node->LRU_link_pre->LRU_link_next = buffer_node->LRU_link_next;
//            buffer_node->LRU_link_next->LRU_link_pre = buffer_node->LRU_link_pre;
//        }
//
//        buffer_node->LRU_link_next = ssd->dram->buffer->buffer_head;
//        ssd->dram->buffer->buffer_head->LRU_link_pre = buffer_node;
//        buffer_node->LRU_link_pre = NULL;
//        ssd->dram->buffer->buffer_head = buffer_node;
//        //ssd->node_move++;
//    }
//}

//struct ssd_info *buffer_warm_up(struct ssd_info *ssd)
//{
//    int device, size, ope, lsn1;
//    int priority;
//    int64_t time_t = 0;
//    unsigned int lsn, lpn, last_lpn, first_lpn, offset;
//    struct buffer_group *buffer_node, key;
//    struct request *new_request = (struct request *)malloc(sizeof(struct request));
//    alloc_assert(new_request, "request");
//
//    ssd->tracefile = fopen(ssd->tracefilename, "r");
//    char buffer[200];
//
//    if(ssd->tracefile==NULL)
//        while(1)
//            printf("trace error\n");
//
//    while(!feof(ssd->tracefile))
//    {
//        fgets(buffer, 200, ssd->tracefile);
//        sscanf(buffer, "%lld %d %d %d %d %d", &time_t, &device, &lsn1, &size, &ope, &priority);
//        //printf("%lld %d %d %d %d %d\n", time_t, device, lsn1, size, ope, priority);
//        if(size==0)
//            while(1)
//                printf("error!");
//
//        memset(new_request, 0, sizeof(struct request));
//
//        new_request->time = time_t;
//        new_request->lsn = lsn1;
//        new_request->size = size;
//        new_request->operation = ope;
//
//        lsn = new_request->lsn; //请求的起始逻辑扇区号
//        lpn = new_request->lsn / ssd->parameter->subpage_page;
//        last_lpn = (new_request->lsn + new_request->size - 1) / ssd->parameter->subpage_page; // last_lpn即该IO请求项操作的最大目标页号
//        first_lpn = new_request->lsn / ssd->parameter->subpage_page;                          // first_lpn即该IO请求项操作的起始目标页号
//
//        if (new_request->operation == READ)
//        {
//            while (lpn <= last_lpn) //当前lpn小于或等于最大目标页号last_lpn时，也就是说明该IO读请求操作尚未完成
//            {
//                offset = get_offset(ssd, new_request, lpn);
//                key.group = lpn;
//                buffer_node = (struct buffer_group *) hash_find(ssd->dram->databuffer, (HASH_NODE *) &key);
//
//                if (buffer_node == NULL)
//                {
//                    //creat_sub_request(ssd, lpn, size(offset), offset, new_request, new_request->operation, 0);
//                    //ssd->dram->buffer->read_miss_hit++;
//                }
//                else
//                {
//                    if ((buffer_node->stored & offset) == offset)
//                    {
//                        //ssd->dram->buffer->read_hit++;
//                        Set_First_warm_up(ssd, buffer_node);
//                    }
//                    else
//                    {
//                        //creat_sub_request(ssd, lpn, size(offset), offset, new_request, new_request->operation, 0);
//                        //ssd->dram->buffer->read_miss_hit++;
//                    }
//                }
//                lpn++;
//            }
//        }
//        else if (new_request->operation == WRITE)
//        {
//            while (lpn <= last_lpn)
//            {
//                key.group = lpn;
//                buffer_node = (struct buffer_group *) hash_find(ssd->dram->databuffer, (HASH_NODE *) &key);
//                offset = get_offset(ssd, new_request, lpn);
//                if (buffer_node != NULL)
//                {
//                    buffer_node->stored = buffer_node->stored | offset;
//                    //ssd->dram->buffer->write_hit++;
//                    Set_First_warm_up(ssd, buffer_node);
//                }
//                else
//                    insert2buffer_warm_up(ssd, lpn, new_request);
//                lpn++;
//            }
//        }
//    }
//    return ssd;
//}

//struct ssd_info *insert2buffer_warm_up(struct ssd_info *ssd, unsigned int lpn, struct request *req)
//{
//    struct buffer_group *buffer_node, *new_node;
//
//    if (ssd->dram->buffer->current_buffer_page>= ssd->dram->buffer->max_buffer_page)
//    {
//        buffer_node = ssd->dram->buffer->buffer_tail;
//
//        ssd->dram->buffer->buffer_tail = ssd->dram->buffer->buffer_tail->LRU_link_pre;
//        ssd->dram->buffer->buffer_tail->LRU_link_next = NULL;
//
//        // ssd->dram->buffer->write_miss_hit++;
//        // ssd->pop_count++;
//        // creat_sub_request(ssd, buffer_node->group, size(buffer_node->stored), buffer_node->stored, req, WRITE, 0);
//        ssd->dram->buffer->current_buffer_page--;
//
//        buffer_node->LRU_link_pre = buffer_node->LRU_link_next = NULL;
//        hash_del(ssd->dram->databuffer, (HASH_NODE *) buffer_node);
//        hash_node_free(ssd->dram->databuffer, (HASH_NODE *) buffer_node);
//        buffer_node = NULL;
//    }
//
//    new_node = NULL;
//    new_node = (struct buffer_group *)malloc(sizeof(struct buffer_group));
//    alloc_assert(new_node, "buffer_group_node");
//    memset(new_node, 0, sizeof(struct buffer_group));
//
//    new_node->group = lpn;                        //把该lpn设置为缓存新节点
//    new_node->stored = get_offset(ssd, req, lpn); //判断扇区是不是在缓存里面，1在，0不在
//    if(new_node->stored==0)
//        while(1)
//            printf("error1\n");
//    new_node->LRU_link_pre = NULL;
//    new_node->LRU_link_next = NULL;
//    if (ssd->dram->buffer->buffer_head == NULL)
//        ssd->dram->buffer->buffer_head = ssd->dram->buffer->buffer_tail = new_node;
//    else
//    {
//        new_node->LRU_link_next = ssd->dram->buffer->buffer_head;
//        ssd->dram->buffer->buffer_head->LRU_link_pre = new_node;
//        ssd->dram->buffer->buffer_head = new_node;
//    }
//    hash_add(ssd->dram->databuffer, (HASH_NODE *) new_node);
//    ssd->dram->databuffer->current_buffer_page++;
//
//    return ssd;
//}
