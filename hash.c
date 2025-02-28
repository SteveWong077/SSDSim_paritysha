#include <stdio.h>
#include <stdlib.h>

#include "hash.h"

#define HASH_CELL_NUM 3

tHash *hash_create(int *freeFunc){
	tHash *pHash = NULL;

	if(!freeFunc)
		return NULL;

	pHash = (tHash *)malloc(sizeof(tHash));
	
	if(pHash != NULL)
	{
		memset((void *)pHash , 0 , sizeof(tHash));
		pHash->free = (void *)freeFunc;
		pHash->nodeArray = NULL;
	}

	return pHash;
}

//
int hash_add(tHash *pHash ,  HASH_NODE *pInsertNode){
	int growthFlag=0 , ret = 0;
	unsigned long long i = 0;
	if(!pHash || !pInsertNode)
		return 0;

	if(pHash->nodeArray == NULL){
		unsigned long long hash_len = ((pHash->max_buffer_page + HASH_CELL_NUM - 1) / HASH_CELL_NUM);
		pHash->nodeArray = malloc(sizeof(HASH_NODE) * hash_len);
		alloc_assert(pHash->nodeArray, "pHash->nodeArray");		
		for(; i < hash_len; ++i){
			pHash->nodeArray[i] = NULL;
		}
	}

	buf_node* node = (buf_node*)pInsertNode;

	i = node->group % pHash->max_buffer_page;//计算哈希索引
	i = i / 3;//分散哈希冲突

	pInsertNode->next = pHash->nodeArray[i];
	pHash->nodeArray[i] = pInsertNode;
	
	pHash->count++;
	return ret;
}


HASH_NODE *hash_find(tHash *pHash, HASH_NODE *pKeyNode){
	if(!pHash || !pHash->count || !pHash->nodeArray)
		return NULL;
	unsigned long long pos = 0;
	buf_node* node = (buf_node*)pKeyNode;
	HASH_NODE *interNode;
	unsigned int target = node->group;
	HASH_NODE *preNode = NULL;
		
	pos = node->group % pHash->max_buffer_page;
	pos /= 3;
	interNode = pHash->nodeArray[pos];
	
	while(interNode){
		node = (buf_node*)interNode;
		if(node->group == target)
			break;
		preNode = interNode;
		interNode = interNode->next;
	}
	if(interNode){
		if(preNode){
			preNode->next = interNode->next;
			interNode->next = pHash->nodeArray[pos];
			pHash->nodeArray[pos] = interNode;
		}
		return interNode;
	}
	
	return NULL;
}

int hash_del(tHash *pHash ,HASH_NODE *pDelNode){
	int ret = 0;
	
	unsigned long long pos = 0;
	buf_node* node = (buf_node*)pDelNode;
	HASH_NODE *interNode;
    //HASH_NODE *Rnode;
	unsigned int target = node->group;
	HASH_NODE *preNode = NULL;
	
	pos = node->group % pHash->max_buffer_page;
	pos /= 3;
	interNode = pHash->nodeArray[pos];
//    Rnode = pHash->nodeArray[pos];
//    while(Rnode){
//        preNode = Rnode;
//        Rnode = Rnode->next;
//    }

//    HASH_NODE *slow = pHash->nodeArray[pos];
//    HASH_NODE *fast = pHash->nodeArray[pos];
//
//    while (fast && fast->next) {
//        slow = slow->next;
//        fast = fast->next->next;
//        if (slow == fast) {
//            printf("Loop detected!\n");
//            break;
//        }
//    }
//    // 如果 fast 或 fast->next 为空，则无环
//    if (!fast || !fast->next) {
//        printf("No cycle found\n");
//    }
//
//    // Step 2: 找到环的起点
//    slow = pHash->nodeArray[pos];  // 让 slow 重新指向头节点
//    while (slow != fast) {
//        slow = slow->next;
//        fast = fast->next;
//    }
//
//    // Step 3: 断开环
//    HASH_NODE *prev = NULL;
//    HASH_NODE *entry = slow;  // 环的起点
//    while (fast->next != entry) {
//        fast = fast->next;
//    }
//
//    // 让 fast 指向 NULL，打破环
//    fast->next = NULL;
//    printf("Cycle removed at group: %u\n", ((buf_node*)entry)->group);



    while(interNode){
		node = (buf_node*)interNode;
//        if(target==45423)
//            break;
        //printf("Checking node: %p, group: %u\n", interNode, node->group); // 打印当前节点的 group 值

		if(node->group == target)
			break;
		preNode = interNode;
		interNode = interNode->next;
	}
	
	if(interNode){
		if(preNode){
			preNode->next = interNode->next;
		}else{
			pHash->nodeArray[pos] = interNode->next;
		}
		pHash->count--;
	}
	
	return 0;
}

void hash_node_free(tHash *pHash, HASH_NODE *pNode){
	if(!pHash || !pNode)
		return;
	free(((struct buffer_group *)pNode)->accessCount);
	free(((struct buffer_group *)pNode)->accessTime);
	(pHash->free)(pNode);
	return ;
}



int hash_destroy(tHash *pHash){
	HASH_NODE *pNode = NULL;
	if(!pHash)
		return 0;

	unsigned long long hash_len = ((pHash->max_buffer_page + HASH_CELL_NUM - 1) / HASH_CELL_NUM);
	int i = 0;
	
	for(;i < hash_len; ++i){
		pNode = pHash->nodeArray[i];
		while(pNode){
			HASH_NODE *willFree = pNode;
			pNode = pNode->next;
			free(willFree);
		}
	}

	free(pHash->nodeArray);
	free(pHash);
	
	return 0;
}


