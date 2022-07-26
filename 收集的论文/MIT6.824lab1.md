---
title: MIT6.824lab1
excerpt: 所在模块：MIT6.824
tags: [MIT6.824]
categories: 后端面试
banner_img: /img/壁纸.jpg
---



1、rpc调用赋值失败

```
ApplyForTask(args *ApplyForTaskArgs, reply *ApplyForTaskReply)
```

正确：

```
*reply = ApplyForTaskReply{
			TaskType:     task.Type,
			TaskIndex:    task.Index,
			MapNum:       c.nMap,
			ReduceNum:    c.nReduce,
			MapInputFile: task.MapInputFile,
		}
```

错误：

```
reply = &ApplyForTaskReply{
			TaskType:     task.Type,
			TaskIndex:    task.Index,
			MapNum:       c.nMap,
			ReduceNum:    c.nReduce,
			MapInputFile: task.MapInputFile,
		}
```





2、遇到死锁

在一个函数A中获得锁，然后在这个A函数调用另一个B函数，这个B函数也需要同一个锁，然后就死锁了。