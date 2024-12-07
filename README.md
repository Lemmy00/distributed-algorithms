# Distributed Algorithms 2023/24 - EPFL

The goal of this practical project is to implement certain building blocks necessary for a decentralized system:

  - Perfect Links (submission #1),
  - FIFO Uniform Reliable Broadcast (submission #2),
  - Lattice Agreement (submission #3)

In this repository, you can find the template for Java and C/C++, but also helpful tools for you to use.
For instructions and details, please refer to the project description.


## Perfect Links commands
```
./run.sh --id 1 --hosts ../example/hosts --output ../example/output/proc01.output ../example/configs/fifo-broadcast.config

./run.sh --id 2 --hosts ../example/hosts --output ../example/output/proc02.output ../example/configs/fifo-broadcast.config

./run.sh --id 3 --hosts ../example/hosts --output ../example/output/proc03.output ../example/configs/fifo-broadcast.config

timeout 1s ./run.sh --id 1 --hosts ../example/hosts --output ../example/output/1.output ../example/configs/perfect-links.config

timeout 120s ./run.sh --id 1 --hosts ../example/hosts --output ../example/output/1.output ../example/configs/perfect-links.config


python3 stress.py fifo -r ../template_cpp/run.sh -l ../example/output/stress -p 128 -m 10000000
python3 stress.py fifo -r ../template_cpp/run.sh -l ../example/output/stress -p 30 -m 500000
python3 stress.py fifo -r ../template_cpp/run.sh -l ../example/output/stress -p 10 -m 10000

python3 stress.py fifo -r ../template_cpp/run.sh -l ../example/output/stress -p 5 -m 10000000
python3 stress.py fifo -r ../template_cpp/run.sh -l ../example/output/stress -p 30 -m 1000000

python3 stress.py fifo -r ../template_cpp/run.sh -l ../example/output/stress -p 7 -m 2000


```