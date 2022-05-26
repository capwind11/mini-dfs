go build -o mini-dfs
nohup ./mini-dfs -app nn -nn-addr 127.0.0.1:8080 > ./log/namenode.log 2>&1 &

# ./mini-dfs -app client -nn-addr 127.0.0.1:8080

