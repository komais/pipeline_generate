# pipeline_generate
usage: pipeline_generate.py [-h] -i INPUT -o OUTDIR

程序描述：
        在串流程的时候，其实是分为三步的，第一步实现每一个小脚本，第二步把相关的小脚本组合成一个模块，完成一个分析功能；第三步，把这些模块类似搭积木一样，进行组合，得到实现不同目标的流程。
        这个程序，就是在大家得到模块后，帮助排列组合的工具。
参数说明：
        -i ： 输入的配置文件，包含如何组合的规则
        -o ： 输出流程的目录
输入文件：搭积木的规则
        包含很多个不同的分析模块，每一模块的其实用[Job Start],终止用[Job End]。在每个模块内部，每一行包括标识符，然后tab分割，相应的值。
        例如：
        Name : 这个分析模块的名称
        Memory：该分析模块的功能，会按照这个设置进行申请内存。如果不能确定，可以先预设得大一些，测试一下流程后，去${OUTDIR}/shell/下去查看相应模块的运行log文件，文件包含该步在计算节点上的内存大小。默认为3G
        Queue : 使用哪个队列，例如组装可以使用super.q,其余的使用sci.q，默认是sci.q
        CPU :　对于这个模块的最大并行任务数，可以是一个整数，那么每次运行则以固定的个数进行投递；也可以是N，表明投递时，同时投递所有的样品；也可以是N/2,N/3,表示为样品数的1/2或者1/3进行投递任务,默认为3
        Major： 是否是主要进程.T - 主干任务， F - 支线任务。大家都玩过游戏，主干任务表示必须得完成这个任务，才能进行下一个任务；而支线任务则不需要。默认是Major
        Order: 第几级任务。
        Thread: 每个任务投递时的线程数，默认不加的时候是1，如果在进行blast或者比对的时候建议根据实际情况加入
        Qsub : 是否投递任务，True使用qsub投递，False直接后台运行，默认是True
        Node:  在False的情况下，投递到那个节点，默认是''
        Command： 命令，如果是多行命令，可以一直往下写，直到遇到[Job End]
        其他：
        OUTDIR：保留字，与最终生成程序的args.outdir对应
        BIN ： 保留字，与最终程序中args.bin对应
        LOGFILE： 保留字，与最终程序中的args.logfile对应
        $(sample) : 表示生成的流程会对args.input 该块的元素进行循环,如果该块有10行，那么会投递10个任务
        Para：生成的流程会对 args.input 中以Para_ 开头的元素支持内插
        DB: 生成的流程会对args.input 中以DB_ 开头的元素也支持内插
        例如：
[Job Start]
Name    MergeFq
Memory  1G
Queue   sci.q
CPU     N
Major   T
Order   1
Thread  5
Qsub    False
Node    c0008
Command make -f BIN/MergeFq/makefile indir=$(sample)[5]/$(sample)[1] outdir=OUTDIR sample_id=$(sample)[0] log_file=LOGFILE LinkFqPara_seq
[Job End]
        上面这个任务处于第一层，major是T，表明要运行第二层任务，必须需要这个任务完成。
输出文件：
        pipeline.py :会在outdir下生成一个pipeline.py , 可以用来运行程序。详细使用见pipeline.py -h
        lib,src : 配置目录，一言难尽
        config_example.txt ： pipeline.py的输入文件的模板。其中[]里面表示一个文本块的起始，直到遇到另一个文本块或者文件结束。其中分成两类：
        一类是[Para],[DB]，这一类是使用该块中变量对pipeline.py中的相应程序进行变量的定义
        另一类是其他名称，例如[sample],表示用该块里面的每一行的相应列进行循环生成脚本，投递任务。
更新说明：
        2015年10月8日
        * 添加了Thread参数，会在qsub-sge.pl中指定线程数，默认为1。 调用的是qsub里面的 -l p=1 。 p为线程数，而不是cpu数。 
          需要注意的是，如果这个值设置的过大，比如10，会导致任务排队时间过长，一般建议在4以下。
        2015年10月14日
        * 删除了 -n 参数，将nohup或者qsub的设置移到config文件中，默认每个任务都是qsub上去，如果有需要设置成登陆节点运行，需要设置Qsub   False
        2016年1月29日
        * 将qsub-sge.pl 转成 qsub_sge.py
        2016年3月30日 v5.6
        * 修改了qsub_sge.py , 修复了du可能无响应的bug，降低了qhost和qstat的频率
        * 将args.bin的绝对路径作为一个值保存下来，存在log.txt中，以便后续查询。
        2016年4月10日 v5.7
        * 添加了Node参数，在非qsub的情况下，可以将任务投递到Node设定的节点后台运行。
To Be Done:
        * 没想到 

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT, --input INPUT
                        input file
  -o OUTDIR, --outdir OUTDIR
                        outdir

author: Liu Tao
mail:   taoliu@annoroad.com
