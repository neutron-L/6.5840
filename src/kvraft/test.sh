#!/bin/bash  
  
# 定义输出文件  
output_file="4.log"  
  
# 定义测试次数或条件，这里以10次为例  
num_tests=100
  
# 清空输出文件（如果存在）  
> "$output_file"  
  
# 使用for循环执行go test  
for ((i=1; i<=$num_tests; i++))  
do  
    echo "Running test $i..."  
    # 执行go test并将结果重定向到输出文件  
    go test > "$output_file" 2>&1  
  
    # 检查输出文件是否包含"FAIL"  
    if grep -q "FAIL" "$output_file"; then  
        echo "Test failed. Exiting loop."  
        break  
    else  
        echo "Test $i passed."  
    fi  
done  
  
# 提示循环结束或失败  
echo "Loop finished or terminated due to failure."