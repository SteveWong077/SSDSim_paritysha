//
//  insert_to_buffer.h
//  RAB
//
//  Created by Lin on 2021/12/3.
//

#ifndef insert_to_buffer_h
#define insert_to_buffer_h

#include <stdio.h>
#include "flash.h"
#include "ssd.h"
#include "initialize.h"
#include "hash.h"
struct ssd_info *buffer_management(struct ssd_info *);
struct ssd_info * insert2buffer(struct ssd_info *ssd,unsigned int lpn,struct sub_request *sub,struct request *req);
struct ssd_info *insert2buffer_warm_up(struct ssd_info *ssd, unsigned int lpn, struct request *req);
unsigned int get_offset(struct ssd_info *ssd, struct request *req, unsigned int lpn);
void Set_First(struct ssd_info *ssd,struct buffer_group *buffer_node);
struct ssd_info *buffer_warm_up(struct ssd_info *);
void Set_First_warm_up(struct ssd_info *ssd, struct buffer_group *buffer_node);
void find_node(struct  ssd_info *ssd,unsigned int lpn,struct request* req);
int find_node_READ(struct  ssd_info *ssd,unsigned int lpn,struct request *req);
#endif /* insert_to_buffer_h */

