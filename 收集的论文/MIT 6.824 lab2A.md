### MIT 6.824 lab2A



1、panic: reflect.Value.Interface: cannot return value obtained from unexported field or method

结构体里面的元素为小写字母，golang默认如果开头为小写的话就不能外部访问。

2、有多个leader

一个投票选出leader之后之前没完成的投票还继续导致选出多个leader

原因：1、一个follow投票了多次

​			2、一个leader选出之后其他follow还继续选举

​			3、一个leader被disconnect了，出来后还以为自己的leader，然后就出现两个leader的错误