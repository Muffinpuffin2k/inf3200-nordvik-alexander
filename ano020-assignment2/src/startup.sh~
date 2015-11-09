#!/bin/bash          
#!/bin/bash          

# Executable
directory="/home/ano020/INF-3200/ano020-assignment2/src" #current working directory
executable="python";
script="backend.py";
# Lists of nodes
nodes=("compute-1-1" "compute-1-2" "compute-1-3" "compute-2-1" "compute-2-2" "compute-2-3" "compute-3-1" "compute-3-2" "compute-3-3" "compute-4-1")

# Stop any running processes
for node in "${nodes[@]}"
do
 # ssh $node bash -c "'pgrep -f '$directory$executable' | xargs kill'"
  ssh $node bash -c "'pgrep -U '$USER' -f '$executable' | xargs kill'"
done

# Boot all processes
for node in "${nodes[@]}"
do
  echo "Booting node" $node
  nohup ssh $node $executable $directory/$script $node> /dev/null 2>&1 &

done

# Wait/Run benchmarks
HEALTY=1
while [ $HEALTY -eq 1  ]; do 
  sleep 1
  echo "Checking if each node is alive and well..."
  for node in "${nodes[@]}"
  do
    if ssh -q $node ps aux | grep $executable > /dev/null 2>&1 ;
    then
      echo "$node is alive"
    else
      echo "$node is dead"
      let HEALTY=0
    fi
  done
done

# Stop 
for node in "${nodes[@]}"
do
  ssh $node bash -c "'pgrep -f '$directory$executable' | xargs kill'"
  if ssh -q $node ps aux | grep $executable > /dev/null 2>&1 ;
  then
    echo "Error: unable to stop $node"
  else
    echo "Shut down $node"
  fi
done
