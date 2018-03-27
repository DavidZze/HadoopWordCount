# HadoopWordCount

可以单机执行，不需要依赖Hadoop-cluster，本地执行，通过设置断点配合IDE Debug 来观察Map, Reduce的处理过程.
hadoop集群是有DataNode和NameNode两种节点构成，DataNode负责存储数据本身而NameNode负责存储数据的元数据信息，在启动mapreduce任务时，数据首先是通过inputformat模块从集群的文件库中读出，然后按照设定的Splitsize进行Split（默认是一个block大小128MB），通过ReadRecorder（RR）将每个split的数据块按行进行轮询访问结果给到map函数，由map函数按照编程的代码逻辑进行处理，输出key和value。由map到reduce的处理过程中包含三件事情，Combiner（map端的预先处理，相对于map段reduce）Partitioner（负责将map输出数据均衡的分配给reduce）Shulffling&&sort(根据map输出的key进行洗牌和排序，将结果根据partitioner的分配情况传输给指定的reduce)，最后reduce按照代码逻辑处理输出结果（也是key,value格式）。

注意：
map阶段的key-value对的格式是由输入的格式所决定的，如果是默认的TextInputFormat，则每行作为一个记录进程处理，其中key为此行的开头相对于文件的起始位置，value就是此行的字符文本。map阶段的输出的key-value对的格式必须同reduce阶段的输入key-value对的格式相对应