"""
这是用于启动Batch处理的综合脚本文件，会被Nodes调用；
当他调用到这个脚本文件的时候，打印信息要求用户手动调用batch/的各个脚本文件，并且当作结束相关内容
"""

print("请手动调用batch/的各个脚本文件，Batch处理目前需要手动进行，相关脚本文件如下：")
print("1. generate_jsonl.py")
print("2. upload_and_start.py")
print("3. download_results.py")
print("4. parse_and_integrate.py")

